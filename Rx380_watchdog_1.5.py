import asyncio
import minimalmodbus
import csv
import logging
import os
from datetime import datetime
import signal
import struct
from pathlib import Path

# Set up logging
logging.basicConfig(filename='rx380_logger.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class GracefulKiller:
    kill_now = False
    pause = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.kill_now = True

class RX380:
    def __init__(self, port='/dev/ttyUSB0', slave_address=1):
        self.instrument = minimalmodbus.Instrument(port, slave_address)
        self.instrument.serial.baudrate = 19200
        self.instrument.serial.bytesize = 8
        self.instrument.serial.parity = minimalmodbus.serial.PARITY_EVEN
        self.instrument.serial.stopbits = 1
        self.instrument.serial.timeout = 1
        self.instrument.mode = minimalmodbus.MODE_RTU

    async def read_scaled_value(self, register_address, scale_factor):
        try:
            raw_value = await asyncio.to_thread(self.instrument.read_registers, register_address, 2, functioncode=4)
            value = (raw_value[0] << 16 | raw_value[1]) * scale_factor
            return value
        except Exception as e:
            logging.error(f"Error reading scaled value from register {register_address}: {e}")
            return None

    async def read_data(self):
        data = {}
        try:
            # Read line voltages (L-L)
            data['voltage_l12'] = await self.read_scaled_value(4028, 0.1)  # V
            data['voltage_l23'] = await self.read_scaled_value(4030, 0.1)  # V
            data['voltage_l31'] = await self.read_scaled_value(4032, 0.1)  # V

            # Read other parameters (not displayed, but saved to CSV)
            data['voltage_l1'] = await self.read_scaled_value(4034, 0.1)  # V
            data['voltage_l2'] = await self.read_scaled_value(4036, 0.1)  # V
            data['voltage_l3'] = await self.read_scaled_value(4038, 0.1)  # V
            data['current_l1'] = await self.read_scaled_value(4020, 0.001)  # A
            data['current_l2'] = await self.read_scaled_value(4022, 0.001)  # A
            data['current_l3'] = await self.read_scaled_value(4024, 0.001)  # A
            data['total_real_power'] = await self.read_long(4012)  # W
            data['total_power_factor'] = await asyncio.to_thread(self.instrument.read_register, 4018, number_of_decimals=3, signed=True, functioncode=4)
            data['frequency'] = await asyncio.to_thread(self.instrument.read_register, 4019, number_of_decimals=2, functioncode=4)  # Hz

            return data
        except Exception as e:
            logging.error(f"Error reading data: {e}")
            return None

    async def read_long(self, register_address):
        try:
            return await asyncio.to_thread(self.instrument.read_long, register_address, functioncode=4, signed=True)
        except Exception as e:
            logging.error(f"Error reading long from register {register_address}: {e}")
            return None

def get_filename(extension):
    today = datetime.now().strftime("%Y-%m-%d")
    return f"rx380_data_{today}.{extension}"

async def save_to_csv(data, folder_path=None):
    if folder_path is None:
        folder_path = Path(os.getenv('CSV_LOG_FOLDER', './logs'))
    folder_path = Path(folder_path)
    folder_path.mkdir(parents=True, exist_ok=True)
    
    filename = folder_path / get_filename("csv")
    file_exists = filename.is_file()
    
    async with asyncio.Lock():
        with open(filename, 'a', newline='') as csvfile:
            fieldnames = ['timestamp'] + list(data[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            for entry in data:
                row_data = {'timestamp': entry['timestamp']}
                row_data.update(entry)
                writer.writerow(row_data)

async def main():
    rx380 = RX380(slave_address=1)
    killer = GracefulKiller()
    
    logging.info("Starting RX380 data logging")
    print("RX380 data logging started. Press Ctrl+C to quit.")
    
    data_buffer = []
    display_counter = 0
    
    try:
        while not killer.kill_now:
            if not killer.pause:
                try:
                    data = await rx380.read_data()
                    if data:
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        data['timestamp'] = timestamp
                        data_buffer.append(data)
                        logging.info("Data read successfully")
                        
                        # Display output on terminal every 2 minutes
                        display_counter += 1
                        if display_counter >= 12:  # 12 * 10 seconds = 2 minutes
                            print(f"\nRX380 Readings at {timestamp}:")
                            print(f"Line Voltage (V): L12={data['voltage_l12']:.1f}, L23={data['voltage_l23']:.1f}, L31={data['voltage_l31']:.1f}")
                            display_counter = 0
                        
                        # Save data every 5 minutes
                        if len(data_buffer) == 30:  # 30 * 10 seconds = 5 minutes
                            await save_to_csv(data_buffer)
                            logging.info("Data saved to CSV file")
                            data_buffer = []
                    else:
                        logging.warning("Failed to read data")
                except Exception as e:
                    logging.error(f"Error in main loop: {e}")
                    print(f"Error: {e}")
            
            await asyncio.sleep(10)  # Read data every 10 seconds
    except asyncio.CancelledError:
        pass
    finally:
        logging.info("Shutting down RX380 data logging")
        print("Shutting down RX380 data logging")

if __name__ == "__main__":
    asyncio.run(main())