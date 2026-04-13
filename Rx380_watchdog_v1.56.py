import asyncio
import minimalmodbus
import csv
import logging
import os
from datetime import datetime, timedelta
import pymssql
import json
from pathlib import Path

# Load configuration from config.json
with open('config.json') as config_file:
    config = json.load(config_file)

# Set up logging
logging.basicConfig(filename='rx380_logger.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

class RX380:
    """Class to handle Modbus communication with RX380 device."""
    def __init__(self, port='/dev/ttyUSB0', slave_address=1):
        self.instrument = minimalmodbus.Instrument(port, slave_address)
        self.setup_instrument()

    def setup_instrument(self):
        """Configure the Modbus instrument settings."""
        self.instrument.serial.baudrate = 19200
        self.instrument.serial.bytesize = 8
        self.instrument.serial.parity = minimalmodbus.serial.PARITY_EVEN
        self.instrument.serial.stopbits = 1
        self.instrument.serial.timeout = 1
        self.instrument.mode = minimalmodbus.MODE_RTU

    async def read_scaled_value(self, register_address, scale_factor):
        """Read and scale values from the Modbus registers."""
        try:
            raw_value = await asyncio.to_thread(
                self.instrument.read_registers, register_address, 2, functioncode=4
            )
            value = (raw_value[0] << 16 | raw_value[1]) * scale_factor
            return value
        except Exception as e:
            logging.error(f"Error reading scaled value from register {register_address}: {e}")
            return None

    async def read_register(self, register_address, number_of_decimals, signed):
        """Read a single register value."""
        try:
            return await asyncio.to_thread(
                self.instrument.read_register, register_address, number_of_decimals, signed=signed, functioncode=4
            )
        except Exception as e:
            logging.error(f"Error reading register {register_address}: {e}")
            return None

    async def read_data(self):
        """Read all necessary data from RX380."""
        data = {}
        try:
            # Read voltages
            data['voltage_l1'] = await self.read_scaled_value(4034, 0.1)  # V
            data['voltage_l2'] = await self.read_scaled_value(4036, 0.1)  # V
            data['voltage_l3'] = await self.read_scaled_value(4038, 0.1)  # V
            data['voltage_l12'] = await self.read_scaled_value(4028, 0.1)  # V
            data['voltage_l23'] = await self.read_scaled_value(4030, 0.1)  # V
            data['voltage_l31'] = await self.read_scaled_value(4032, 0.1)  # V

            # Read max voltages
            data['voltage_l12_max'] = await self.read_scaled_value(4124, 0.1)  # V
            data['voltage_l23_max'] = await self.read_scaled_value(4128, 0.1)  # V
            data['voltage_l31_max'] = await self.read_scaled_value(4132, 0.1)  # V

            # Read min voltages
            data['voltage_l12_min'] = await self.read_scaled_value(4212, 0.1)  # V
            data['voltage_l23_min'] = await self.read_scaled_value(4216, 0.1)  # V
            data['voltage_l31_min'] = await self.read_scaled_value(4220, 0.1)  # V

            # Read current
            data['current_l1'] = await self.read_scaled_value(4020, 0.001)  # A
            data['current_l2'] = await self.read_scaled_value(4022, 0.001)  # A
            data['current_l3'] = await self.read_scaled_value(4024, 0.001)  # A
            data['current_ln'] = await self.read_scaled_value(4026, 0.001)  # A

            # Read power
            data['total_real_power'] = await self.read_scaled_value(4012, 1)  # W
            data['total_apparent_power'] = await self.read_scaled_value(4014, 1)  # VA
            data['total_reactive_power'] = await self.read_scaled_value(4016, 1)  # VAR

            # Read power factor and frequency
            data['total_power_factor'] = await self.read_register(4018, 3, True)
            data['frequency'] = await self.read_register(4019, 2, False)  # Hz

            # Read energy
            data['total_real_energy'] = await self.read_scaled_value(4002, 1)  # kWh
            data['total_reactive_energy'] = await self.read_scaled_value(4010, 1)  # kVARh
            data['total_apparent_energy'] = await self.read_scaled_value(4006, 1)  # kVAh

            return data
        except Exception as e:
            logging.error(f"Error reading data: {e}")
            return None

class DataManager:
    """Class to handle data storage in SQL and CSV."""
    def __init__(self):
        self.db_config = {
            'server': os.getenv('DB_SERVER', 'YOUR_DB_SERVER_IP'),
            'database': os.getenv('DB_NAME', 'YOUR_DB_NAME'),
            'user': os.getenv('DB_USER', 'YOUR_DB_USER'),
            'password': os.getenv('DB_PASSWORD', 'YOUR_DB_PASSWORD')
        }

    async def save_to_sql(self, data_buffer):
        """Save data to SQL Server with error handling."""
        insert_query = """
        INSERT INTO Office_Readings 
           (Timestamp, VoltageL1_v, VoltageL2_v, VoltageL3_v, VoltageL12_v, VoltageL23_v, VoltageL31_v,
            VoltageL12_maxv, VoltageL23_maxv, VoltageL31_maxv, VoltageL12_minv, VoltageL23_minv, VoltageL31_minv,
            CurrentL1_I, CurrentL2_I, CurrentL3_I, CurrentLn_I,
            TotalRealPower_kWh, TotalApparentPower_kWh, TotalReactivePower_kWh, TotalPowerFactor_kWh, Frequency,
            TotalRealEnergy, TotalReactiveEnergy, TotalApparentEnergy)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            conn = await asyncio.to_thread(pymssql.connect, **self.db_config)
            cursor = conn.cursor()
            for data in data_buffer:
                values = (
                    data['timestamp'],
                    data['voltage_l1'], data['voltage_l2'], data['voltage_l3'],
                    data['voltage_l12'], data['voltage_l23'], data['voltage_l31'],
                    data['voltage_l12_max'], data['voltage_l23_max'], data['voltage_l31_max'],
                    data['voltage_l12_min'], data['voltage_l23_min'], data['voltage_l31_min'],
                    data['current_l1'], data['current_l2'], data['current_l3'], data['current_ln'],
                    data['total_real_power'] / 1000, data['total_apparent_power'] / 1000, data['total_reactive_power'] / 1000,
                    data['total_power_factor'], data['frequency'],
                    data['total_real_energy'], data['total_reactive_energy'], data['total_apparent_energy']
                )
                await asyncio.to_thread(cursor.execute, insert_query, values)
            await asyncio.to_thread(conn.commit)
            logging.info(f"Data inserted successfully into SQL Server! ({len(data_buffer)} records)")
        except Exception as e:
            logging.error(f"Error inserting data into SQL Server: {e}")
            if 'conn' in locals():
                await asyncio.to_thread(conn.rollback)
        finally:
            if 'cursor' in locals():
                await asyncio.to_thread(cursor.close)
            if 'conn' in locals():
                await asyncio.to_thread(conn.close)

def get_filename(extension):
    """Generate a filename based on the current date."""
    today = datetime.now().strftime("%Y-%m-%d")
    return f"rx380_data_{today}.{extension}"

async def save_to_csv(data, folder_path=None):
    """Save a single data point to a CSV file."""
    if folder_path is None:
        folder_path = Path(os.getenv('CSV_LOG_FOLDER', './logs'))
    folder_path = Path(folder_path)
    folder_path.mkdir(parents=True, exist_ok=True)
    
    filename = folder_path / get_filename("csv")
    file_exists = filename.is_file()
    
    async with asyncio.Lock():
        with open(filename, 'a', newline='') as csvfile:
            fieldnames = ['timestamp'] + list(data.keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerow(data)
    
    logging.info(f"Data saved to CSV file: {filename}")

async def main():
    """Main function to handle data reading, saving, and logging."""
    rx380 = RX380(slave_address=1)
    data_manager = DataManager()
    
    logging.info("Starting RX380 data logging")
    print("RX380 data logging started.")
    
    try:
        # Calculate the next 10-minute mark
        now = datetime.now()
        next_save_time = (now + timedelta(minutes=10 - now.minute % 10)).replace(second=0, microsecond=0)
        logging.info(f"Next data save scheduled at {next_save_time}")

        while True:
            # Wait until the next 10-minute mark
            now = datetime.now()
            wait_time = (next_save_time - now).total_seconds()
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            
            # Collect and save a single data point
            data = await rx380.read_data()
            if data:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                data['timestamp'] = timestamp
                logging.info("Data read successfully")

                # Save data to SQL and CSV
                await data_manager.save_to_sql([data])  # Pass as a list with one data point
                await save_to_csv(data)
                logging.info(f"Data saved at {timestamp}")

                # Schedule the next save time
                next_save_time = (datetime.now() + timedelta(minutes=10)).replace(second=0, microsecond=0)
                logging.info(f"Next data save scheduled at {next_save_time}")

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        print("Program interrupted by user. Shutting down...")
    finally:
        logging.info("Shutting down RX380 data logging")
        print("RX380 data logging shut down.")

if __name__ == "__main__":
    asyncio.run(main())