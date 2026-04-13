import asyncio
import minimalmodbus
import csv
import logging
import os
from datetime import datetime
from pyexcel_ods3 import save_data, get_data
from collections import OrderedDict, deque
import signal
import struct
from pathlib import Path
import pymssql

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
            raise

    async def read_float(self, register_address):
        try:
            raw_value = await asyncio.to_thread(self.instrument.read_long, register_address, functioncode=4)
            return struct.unpack('>f', struct.pack('>I', raw_value))[0]
        except Exception as e:
            logging.error(f"Error reading float from register {register_address}: {e}")
            raise

    async def read_long(self, register_address, signed=True):
        try:
            return await asyncio.to_thread(self.instrument.read_long, register_address, functioncode=4, signed=signed)
        except Exception as e:
            logging.error(f"Error reading long from register {register_address}: {e}")
            raise

    async def read_register(self, register_address, number_of_decimals, signed):
        try:
            return await asyncio.to_thread(self.instrument.read_register, register_address, number_of_decimals=number_of_decimals, signed=signed, functioncode=4)
        except Exception as e:
            logging.error(f"Error reading register {register_address}: {e}")
            raise

    async def read_data(self):
        data = {}
        try:
            # Read phase voltages (L-N)
            data['voltage_l1'] = await self.read_scaled_value(4034, 0.1)  # V
            data['voltage_l2'] = await self.read_scaled_value(4036, 0.1)  # V
            data['voltage_l3'] = await self.read_scaled_value(4038, 0.1)  # V

            # Read line voltages (L-L)
            data['voltage_l12'] = await self.read_scaled_value(4028, 0.1)  # V
            data['voltage_l23'] = await self.read_scaled_value(4030, 0.1)  # V
            data['voltage_l31'] = await self.read_scaled_value(4032, 0.1)  # V

            # Read current
            data['current_l1'] = await self.read_scaled_value(4020, 0.001)  # A
            data['current_l2'] = await self.read_scaled_value(4022, 0.001)  # A
            data['current_l3'] = await self.read_scaled_value(4024, 0.001)  # A
            data['current_ln'] = await self.read_scaled_value(4026, 0.001)  # A

            # Read power
            data['total_real_power'] = await self.read_long(4012)  # W
            data['total_apparent_power'] = await self.read_long(4014, signed=False)  # VA
            data['total_reactive_power'] = await self.read_long(4016)  # VAR

            # Read power factor and frequency
            data['total_power_factor'] = await self.read_register(4018, 3, True)
            data['frequency'] = await self.read_register(4019, 2, False)  # Hz

            # Read energy
            data['total_real_energy'] = await self.read_long(4002, signed=False)  # kWh
            data['total_reactive_energy'] = await self.read_long(4010, signed=False)  # kVARh
            data['total_apparent_energy'] = await self.read_long(4006, signed=False)  # kVAh

            return data
        except Exception as e:
            logging.error(f"Error reading data: {e}")
            return None

class DataManager:
    def __init__(self, buffer_size=720):  # 720 * 5 minutes = 60 hours of data
        self.buffer = deque(maxlen=buffer_size)
        self.folder_path = Path(os.getenv('CSV_LOG_FOLDER', './logs'))
        self.folder_path.mkdir(parents=True, exist_ok=True)
        self.db_config = {
            'server': os.getenv('DB_SERVER', 'YOUR_DB_SERVER_IP'),
            'database': os.getenv('DB_NAME', 'YOUR_DB_NAME'),
            'user': os.getenv('DB_USER', 'YOUR_DB_USER'),
            'password': os.getenv('DB_PASSWORD', 'YOUR_DB_PASSWORD')
        }
        
    def add_data(self, data):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.buffer.append({'timestamp': timestamp, **data})

    async def save_to_csv(self):
        filename = self.folder_path / f"rx380_data_{datetime.now().strftime('%Y-%m-%d')}.csv"
        file_exists = filename.is_file()

        try:
            with open(filename, 'a', newline='') as csvfile:
                fieldnames = ['timestamp'] + list(self.buffer[0].keys() - {'timestamp'})
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                if not file_exists:
                    writer.writeheader()

                writer.writerows(self.buffer)
            logging.info(f"Data saved to CSV: {filename}")
        except Exception as e:
            logging.error(f"Error saving to CSV: {e}")

    async def save_to_ods(self):
        filename = self.folder_path / f"rx380_data_{datetime.now().strftime('%Y-%m-%d')}.ods"

        try:
            if filename.is_file():
                sheet_data = await asyncio.to_thread(get_data, str(filename))
                sheet = sheet_data["Sheet1"]
            else:
                sheet = [['timestamp'] + list(self.buffer[0].keys() - {'timestamp'})]

            for data in self.buffer:
                row_data = [data['timestamp']] + [data[key] for key in sheet[0][1:]]
                sheet.append(row_data)

            await asyncio.to_thread(save_data, str(filename), OrderedDict([("Sheet1", sheet)]))
            logging.info(f"Data saved to ODS: {filename}")
        except Exception as e:
            logging.error(f"Error saving to ODS: {e}")

    async def save_to_sql(self, data):
        insert_query = """
        INSERT INTO Office_Readings 
           (VoltageL1_v, VoltageL2_v, VoltageL3_v, CurrentL1_I, CurrentL2_I, CurrentL3_I, CurrentLn_I,
            TotalRealPower_kWh, TotalApparentPower_kWh, TotalReactivePower_kWh, TotalPowerFactor_kWh, Frequency_,
            TotalRealEnergy, TotalReactiveEnergy, TotalApparentEnergy)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            data['voltage_l1'], data['voltage_l2'], data['voltage_l3'],
            data['current_l1'], data['current_l2'], data['current_l3'], data['current_ln'],
            data['total_real_power'] / 1000, data['total_apparent_power'] / 1000, data['total_reactive_power'] / 1000,
            data['total_power_factor'], data['frequency'],
            data['total_real_energy'], data['total_reactive_energy'], data['total_apparent_energy']
        )

        try:
            conn = await asyncio.to_thread(pymssql.connect, **self.db_config)
            cursor = conn.cursor()
            await asyncio.to_thread(cursor.execute, insert_query, values)
            await asyncio.to_thread(conn.commit)
            logging.info("Data inserted successfully into SQL Server!")
        except Exception as e:
            logging.error(f"Error inserting data into SQL Server: {e}")
            if 'conn' in locals():
                await asyncio.to_thread(conn.rollback)
        finally:
            if 'cursor' in locals():
                await asyncio.to_thread(cursor.close)
            if 'conn' in locals():
                await asyncio.to_thread(conn.close)

    async def save_data(self):
        tasks = [
            self.save_to_csv(),
            self.save_to_ods()
        ]
        
        # Save the last data point to SQL Server
        if self.buffer:
            tasks.append(self.save_to_sql(self.buffer[-1]))
        
        await asyncio.gather(*tasks)

async def user_input_handler(killer):
    while not killer.kill_now:
        user_input = await asyncio.to_thread(input)
        user_input = user_input.lower()
        if user_input == 'q':
            print("Quitting...")
            killer.kill_now = True
        elif user_input == 'w':
            print("Pausing...")
            killer.pause = True
        elif user_input == 'r':
            print("Resuming...")
            killer.pause = False

async def main():
    rx380 = RX380(slave_address=1)
    data_manager = DataManager()
    killer = GracefulKiller()

    logging.info("Starting RX380 data logging")
    print("RX380 data logging started. Press 'q' to quit, 'w' to pause, 'r' to resume.")

    input_task = asyncio.create_task(user_input_handler(killer))

    try:
        while not killer.kill_now:
            if not killer.pause:
                try:
                    data = await rx380.read_data()
                    if data:
                        logging.info("Data read successfully")
                        data_manager.add_data(data)

                        # Display output on terminal
                        print("\nRX380 Readings:")
                        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"Phase Voltage (V): L1={data['voltage_l1']:.1f}, L2={data['voltage_l2']:.1f}, L3={data['voltage_l3']:.1f}")
                        print(f"Line Voltage (V): L12={data['voltage_l12']:.1f}, L23={data['voltage_l23']:.1f}, L31={data['voltage_l31']:.1f}")
                        print(f"Current (A): L1={data['current_l1']:.2f}, L2={data['current_l2']:.2f}, L3={data['current_l3']:.2f}")
                        print(f"Total Real Power: {data['total_real_power']} W")
                        print(f"Total Power Factor: {data['total_power_factor']:.3f}")
                        print(f"Frequency: {data['frequency']:.2f} Hz")
                    else:
                        logging.warning("Failed to read data")
                        print("Failed to read data")
                except Exception as e:
                    logging.error(f"Error in main loop: {e}")
                    print(f"Error: {e}")

            # Save data every 5 minutes
            if len(data_manager.buffer) % 5 == 0:
                await data_manager.save_data()

            await asyncio.sleep(60)  # Wait for 1 minute before next reading

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.critical(f"Critical error in main function: {e}")
        print(f"Critical error: {e}")
    finally:
        input_task.cancel()
        await data_manager.save_data()  # Save any remaining data
        logging.info("Shutting down RX380 data logging")
        print("Shutting down RX380 data logging")

if __name__ == "__main__":
    asyncio.run(main())