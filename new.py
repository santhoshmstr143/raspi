import os, csv, time, serial, uuid, subprocess
from datetime import datetime
from threading import Thread, Lock, Event
from smbus2 import SMBus
from picamera2 import Picamera2
from supabase import create_client

# ========= Supabase Config =========
SUPABASE_URL = "https://ghtqafnlnijxvsmzdnmh.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdodHFhZm5sbmlqeHZzbXpkbm1oIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTk3NDQyNjcsImV4cCI6MjA3NTMyMDI2N30.Q1LGQP8JQdWn6rJJ1XRYT8rfo9b2Q5YfWUytrzQEsa0"

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ========= Hardware Config =========
BUS_NUM = 1
ADDR = 0x68
PWR_MGMT_1 = 0x6B
ACCEL_XOUT_H = 0x3B
GYRO_XOUT_H = 0x43
GPS_PORT = "/dev/serial0"
GPS_BAUD = 9600
FPS = 5  # Frames per second for camera + IMU

# ========= Globals =========
running = False
rider_id = None
current_file_id = None
current_folder = None
gps_lock = Lock()
latest_gps = {
    "utc": "N/A", "lat": "N/A", "ns": "N/A",
    "lon": "N/A", "ew": "N/A", "speed": "N/A",
    "course": "N/A", "date": "N/A", "valid": "N/A"
}

# Thread references
gps_thread_obj = None
cam_imu_thread_obj = None
stop_event = Event()  # Signal to stop threads

# Camera object global reference
picam2_global = None
camera_lock = Lock()

# ========= Camera Reset Function =========
def reset_camera():
    """Reset camera by killing any existing processes"""
    try:
        print("🔄 Resetting camera processes...")
        subprocess.run(['sudo', 'killall', '-9', 'libcamera-hello'], 
                      capture_output=True, stderr=subprocess.DEVNULL)
        subprocess.run(['sudo', 'killall', '-9', 'libcamera-still'], 
                      capture_output=True, stderr=subprocess.DEVNULL)
        subprocess.run(['sudo', 'killall', '-9', 'libcamera-vid'], 
                      capture_output=True, stderr=subprocess.DEVNULL)
        subprocess.run(['sudo', 'pkill', '-9', '-f', 'libcamera'], 
                      capture_output=True, stderr=subprocess.DEVNULL)
        time.sleep(3)  # Give more time for cleanup
        print("✅ Camera processes reset complete")
    except Exception as e:
        print(f"⚠️  Camera reset error (non-critical): {e}")

def cleanup_camera():
    """Properly cleanup camera object"""
    global picam2_global
    with camera_lock:
        if picam2_global is not None:
            try:
                print("🛑 Stopping camera...")
                picam2_global.stop()
                picam2_global.close()
                time.sleep(1)
                print("✅ Camera stopped and closed")
            except Exception as e:
                print(f"⚠️  Camera cleanup error: {e}")
            finally:
                picam2_global = None

# ========= Helpers =========
def read_word(bus, addr, reg):
    try:
        high = bus.read_byte_data(addr, reg)
        low  = bus.read_byte_data(addr, reg+1)
        val = (high << 8) | low
        if val & 0x8000:
            val -= 0x10000
        return val
    except Exception as e:
        print(f"IMU read error: {e}")
        return 0

def setup_data_folder():
    main_data_dir = "data"
    os.makedirs(main_data_dir, exist_ok=True)
    
    n = 1
    while os.path.exists(os.path.join(main_data_dir, f"ride{n:02d}")):
        n += 1
    
    session_folder = os.path.join(main_data_dir, f"ride{n:02d}")
    os.makedirs(session_folder, exist_ok=True)
    os.makedirs(os.path.join(session_folder, "images"), exist_ok=True)
    
    return session_folder

def parse_gprmc(line):
    try:
        parts = line.split(",")
        if len(parts) >= 10:
            return {
                "utc": parts[1] or "N/A",
                "valid": parts[2] or "N/A",
                "lat": parts[3] or "N/A",
                "ns": parts[4] or "N/A",
                "lon": parts[5] or "N/A",
                "ew": parts[6] or "N/A",
                "speed": parts[7] or "N/A",
                "course": parts[8] or "N/A",
                "date": parts[9] or "N/A"
            }
    except:
        pass
    return None

# ========= Command Listener Thread =========
def command_listener():
    """Listen for START/STOP commands from Supabase"""
    global running, current_file_id, rider_id, current_folder
    global gps_thread_obj, cam_imu_thread_obj, stop_event

    last_command_id = None
    print("👂 Command listener started - waiting for commands...")

    while True:
        try:
            # Fetch latest pending command (from any rider)
            result = supabase.table("rider_commands")\
                .select("*")\
                .eq("status", "pending")\
                .order("timestamp", desc=True)\
                .limit(1)\
                .execute()

            if result.data:
                latest = result.data[0]
                
                # Check if this is a new command
                if latest["id"] != last_command_id:
                    last_command_id = latest["id"]
                    command = latest["command"].lower()
                    
                    # Get rider_id from the command itself
                    rider_id = latest["rider_id"]

                    if command == "start" and not running:
                        running = True
                        stop_event.clear()  # Reset stop signal
                        
                        # Create new data folder
                        current_folder = setup_data_folder()
                        
                        # Create riderfiles entry
                        file_res = supabase.table("riderfiles").insert({
                            "rider_id": rider_id,
                            "filename": f"ride_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            "filepath": current_folder
                        }).execute()
                        current_file_id = file_res.data[0]["id"]

                        print(f"\n{'='*60}")
                        print(f"🚀 START command received!")
                        print(f"👤 Rider ID: {rider_id}")
                        print(f"📄 File ID: {current_file_id}")
                        print(f"📁 Data folder: {current_folder}")
                        print(f"{'='*60}\n")

                        # START GPS THREAD
                        gps_thread_obj = Thread(target=gps_thread, daemon=False)
                        gps_thread_obj.start()
                        print("📡  GPS thread STARTED")

                        # START CAMERA/IMU THREAD
                        cam_imu_thread_obj = Thread(target=cam_imu_thread, daemon=False)
                        cam_imu_thread_obj.start()
                        print("📷 Camera/IMU thread STARTED")

                        # Mark command as executed
                        supabase.table("rider_commands")\
                            .update({"status": "executed"})\
                            .eq("id", latest["id"])\
                            .execute()

                    elif command == "stop" and running:
                        running = False
                        stop_event.set()  # Signal threads to stop
                        
                        print(f"\n{'='*60}")
                        print(f"🛑 STOP command received!")
                        print(f"👤 Rider ID: {rider_id}")
                        print(f"⏹️ Stopping threads...")
                        print(f"{'='*60}\n")
                        
                        # Wait for threads to finish
                        if gps_thread_obj and gps_thread_obj.is_alive():
                            gps_thread_obj.join(timeout=5)
                            print("✅ GPS thread STOPPED")
                        
                        if cam_imu_thread_obj and cam_imu_thread_obj.is_alive():
                            cam_imu_thread_obj.join(timeout=10)
                            print("✅ Camera/IMU thread STOPPED")
                        
                        # Cleanup camera completely
                        cleanup_camera()
                        
                        # Reset camera processes for next ride
                        reset_camera()
                        
                        print(f"💾 Data saved in: {current_folder}\n")
                        
                        # Mark command as executed
                        supabase.table("rider_commands")\
                            .update({"status": "executed"})\
                            .eq("id", latest["id"])\
                            .execute()
                        
                        current_file_id = None
                        current_folder = None
                        gps_thread_obj = None
                        cam_imu_thread_obj = None
                        
                        print("🔄 System ready for next ride\n")

        except Exception as e:
            print(f"❌  Command listener error: {e}")
        
        time.sleep(1)  # Check for commands every second

# ========= GPS Thread =========
def gps_thread():
    """GPS data collection thread - stops when stop_event is set"""
    print("📡  GPS collection starting...")
    
    try:
        with serial.Serial(GPS_PORT, GPS_BAUD, timeout=1) as ser:
            while not stop_event.is_set():
                try:
                    line = ser.readline().decode("ascii", errors="ignore").strip()
                    if line.startswith("$GPRMC"):
                        gps_data = parse_gprmc(line)
                        if gps_data:
                            with gps_lock:
                                latest_gps.update(gps_data)
                except Exception as e:
                    if not stop_event.is_set():
                        print(f"GPS error: {e}")
                    time.sleep(1)
    except Exception as e:
        print(f"GPS serial error: {e}")
    
    print("📡  GPS thread exiting...")

# ========= Camera + IMU Thread =========
def cam_imu_thread():
    """Combined camera and IMU thread - stops when stop_event is set"""
    global picam2_global
    
    print("📷 Camera/IMU initialization starting...")

    # RESET CAMERA BEFORE INITIALIZATION
    reset_camera()

    # Setup IMU
    bus = None
    try:
        bus = SMBus(BUS_NUM)
        bus.write_byte_data(ADDR, PWR_MGMT_1, 0)
        time.sleep(0.1)
        print("✅ IMU initialized successfully")
    except Exception as e:
        print(f"❌ IMU initialization error: {e}")
        return

    # Setup camera with proper error handling
    try:
        with camera_lock:
            print("📷 Creating Picamera2 instance...")
            picam2_global = Picamera2()
            
            print("📷 Configuring camera...")
            config = picam2_global.create_video_configuration(
                main={"size": (1920, 1080), "format": "RGB888"}
            )
            picam2_global.configure(config)
            
            print("📷 Starting camera...")
            picam2_global.start()
            
        time.sleep(3)  # Give camera more time to fully initialize
        print("✅ Camera initialized successfully")
        
    except Exception as e:
        print(f"❌ Camera initialization error: {e}")
        if bus:
            bus.close()
        return

    csv_file = None
    csv_writer = None
    img_count = 0

    try:
        # Open CSV file
        csv_path = os.path.join(current_folder, "combined_data.csv")
        csv_file = open(csv_path, "w", newline="")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([
            "timestamp", "epoch_time", "image_filename",
            "ax_g", "ay_g", "az_g",
            "gx_dps", "gy_dps", "gz_dps",
            "gps_utc", "gps_lat", "gps_ns", "gps_lon", "gps_ew",
            "gps_speed_kn", "gps_course_deg", "gps_valid"
        ])
        print(f"📝 CSV file created: {csv_path}")

        while not stop_event.is_set():
            # Get synchronized timestamp
            now = datetime.now()
            timestamp_str = now.strftime("%Y-%m-%d_%H-%M-%S-%f")[:-3]
            epoch_time = time.time()

            # Capture image
            img_count += 1
            filename = f"image_{timestamp_str}_{img_count:04d}.jpg"
            filepath = os.path.join(current_folder, "images", filename)
            
            try:
                with camera_lock:
                    if picam2_global is not None:
                        picam2_global.capture_file(filepath)
            except Exception as e:
                if not stop_event.is_set():
                    print(f"⚠️  Camera capture error: {e}")
                filename = "capture_failed.jpg"

            # Read IMU data
            ax = read_word(bus, ADDR, ACCEL_XOUT_H)
            ay = read_word(bus, ADDR, ACCEL_XOUT_H+2)
            az = read_word(bus, ADDR, ACCEL_XOUT_H+4)
            gx = read_word(bus, ADDR, GYRO_XOUT_H)
            gy = read_word(bus, ADDR, GYRO_XOUT_H+2)
            gz = read_word(bus, ADDR, GYRO_XOUT_H+4)

            # Convert to physical units
            ax_g = ax / 16384.0
            ay_g = ay / 16384.0
            az_g = az / 16384.0
            gx_dps = gx / 131.0
            gy_dps = gy / 131.0
            gz_dps = gz / 131.0

            # Get latest GPS data safely
            with gps_lock:
                gps_copy = latest_gps.copy()

            # Write to CSV backup
            csv_writer.writerow([
                timestamp_str, epoch_time, f"images/{filename}",
                round(ax_g, 4), round(ay_g, 4), round(az_g, 4),
                round(gx_dps, 3), round(gy_dps, 3), round(gz_dps, 3),
                gps_copy["utc"], gps_copy["lat"], gps_copy["ns"],
                gps_copy["lon"], gps_copy["ew"],
                gps_copy["speed"], gps_copy["course"], gps_copy["valid"]
            ])
            csv_file.flush()

            # ========= PUSH TO SUPABASE IN REAL-TIME =========
            try:
                # Convert GPS speed and course to float
                speed_val = None
                course_val = None
                
                if gps_copy["speed"] != "N/A":
                    try:
                        speed_val = float(gps_copy["speed"])
                    except:
                        pass
                
                if gps_copy["course"] != "N/A":
                    try:
                        course_val = float(gps_copy["course"])
                    except:
                        pass

                # Insert sensor reading to Supabase
                supabase.table("riderdata").insert({
                    "rider_id": rider_id,
                    "file_id": current_file_id,
                    "timestamp": now.isoformat(),
                    "ax": round(ax_g, 4),
                    "ay": round(ay_g, 4),
                    "az": round(az_g, 4),
                    "gx": round(gx_dps, 3),
                    "gy": round(gy_dps, 3),
                    "gz": round(gz_dps, 3),
                    "gps_utc": gps_copy["utc"],
                    "gps_lat": gps_copy["lat"],
                    "gps_ns": gps_copy["ns"],
                    "gps_lon": gps_copy["lon"],
                    "gps_ew": gps_copy["ew"],
                    "gps_speed_kn": speed_val,
                    "gps_course_deg": course_val,
                    "gps_valid": gps_copy["valid"],
                    "image_filename": f"images/{filename}"
                }).execute()

                print(f"✅ [{timestamp_str}] Data pushed to Supabase")

            except Exception as e:
                if not stop_event.is_set():
                    print(f"❌  Supabase push error: {e}")

            # Console output
            print(f"📷 Image: {filename}")
            print(f"   IMU -> ACC: {ax_g:.3f}, {ay_g:.3f}, {az_g:.3f} | "
                  f"GYRO: {gx_dps:.3f}, {gy_dps:.3f}, {gz_dps:.3f}")
            print(f"   GPS -> Lat:{gps_copy['lat']}{gps_copy['ns']} | "
                  f"Lon:{gps_copy['lon']}{gps_copy['ew']} | Speed:{gps_copy['speed']}kn | Valid:{gps_copy['valid']}")
            print("-" * 100)

            # Control frame rate
            time.sleep(1.0 / FPS)

    except Exception as e:
        if not stop_event.is_set():
            print(f"❌ Main loop error: {e}")
    finally:
        # Cleanup
        try:
            if csv_file:
                csv_file.close()
                print("💾 CSV file closed")
        except Exception as e:
            print(f"CSV cleanup error: {e}")
        
        try:
            with camera_lock:
                if picam2_global is not None:
                    picam2_global.stop()
                    print("📷 Camera stopped in thread")
        except Exception as e:
            print(f"Camera stop error: {e}")
        
        try:
            if bus:
                bus.close()
                print("🔌 IMU bus closed")
        except Exception as e:
            print(f"IMU cleanup error: {e}")
    
    print("📷 Camera/IMU thread exiting...")

# ========= Main =========
if __name__ == "__main__":
    print("=" * 60)
    print("   IMU + Camera + GPS Data Logger with Supabase")
    print("=" * 60)
    print(f"☁️  Supabase: Real-time updates enabled")
    print(f"🎧 Listening for commands from ANY rider")
    print("-" * 60)
    
    # Initial camera cleanup on startup
    reset_camera()
    
    try:
        # Only start command listener thread
        t_cmd = Thread(target=command_listener, daemon=True)
        print("🚀 Starting command listener...")
        t_cmd.start()
        
        print("\n⏳  WAITING FOR START COMMAND FROM WEBSITE...")
        print("   GPS and Camera threads will start when START is received")
        print("=" * 60)
        
        # Keep main thread alive
        while True:
            time.sleep(1)
        
    except KeyboardInterrupt:
        print("\n⛔ Received stop signal...")
        stop_event.set()  # Signal all threads to stop
        cleanup_camera()
        reset_camera()
        print("👋 Exiting program...")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        cleanup_camera()
        print("✅ Program ended.")