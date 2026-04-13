#!/usr/bin/env python3
import asyncio
import csv
import json
import logging
import os
import minimalmodbus
import pymssql
from pathlib import Path
from datetime import datetime, timedelta
import aiofiles

# -------------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------------
config = {
    "modbus": {
        "port": "/dev/ttyUSB0",
        "slave_address": 1,
        "baudrate": 19200
    },
    "database": {
        "server": os.getenv("DB_SERVER", "YOUR_DB_SERVER_IP"),
        "database": os.getenv("DB_NAME", "YOUR_DB_NAME"),
        "user": os.getenv("DB_USER", "YOUR_DB_USER"),
        "password": os.getenv("DB_PASSWORD", "YOUR_DB_PASSWORD")
    },
    "csv": {
        "log_folder": os.getenv("CSV_LOG_FOLDER", "./logs")
    },
    "logging": {
        "log_file": "rx380_logger.log",
        "level": "INFO"
    },
    "data_save_interval": {
        "minutes": 10
    }
}

# -------------------------------------------------------------------------------
# Logger Setup
# -------------------------------------------------------------------------------
def setup_logger(cfg):
    level = getattr(logging, cfg.get("level", "INFO").upper(), logging.INFO)
    logging.basicConfig(
        filename=cfg.get("log_file", "app.log"),
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    console = logging.StreamHandler()
    console.setLevel(level)
    logging.getLogger('').addHandler(console)
    return logging.getLogger()

logger = setup_logger(config["logging"])
logger.info("Logger initialized.")

# -------------------------------------------------------------------------------
# Helper function to compute next save time aligned with the clock
# -------------------------------------------------------------------------------
def compute_next_save_time(now, interval_minutes):
    minutes_since_midnight = now.hour * 60 + now.minute
    next_interval = ((minutes_since_midnight // interval_minutes) + 1) * interval_minutes
    next_time = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(minutes=next_interval)
    return next_time

# -------------------------------------------------------------------------------
# Modbus Client Module
# -------------------------------------------------------------------------------
class ModbusClient:
    def __init__(self, cfg, logger: logging.Logger):
        self.port = cfg["port"]
        self.slave_address = cfg["slave_address"]
        self.baudrate = cfg["baudrate"]
        self.logger = logger
        self.instrument = minimalmodbus.Instrument(self.port, self.slave_address)
        self.setup_instrument()
        
    def setup_instrument(self):
        self.instrument.serial.baudrate = self.baudrate
        self.instrument.serial.bytesize = 8
        self.instrument.serial.parity = minimalmodbus.serial.PARITY_EVEN
        self.instrument.serial.stopbits = 1
        self.instrument.serial.timeout = 1
        self.instrument.mode = minimalmodbus.MODE_RTU
        self.logger.info("Modbus instrument set up.")

    async def read_scaled_value(self, register_address, scale_factor):
        try:
            raw_value = await asyncio.to_thread(
                self.instrument.read_registers, register_address, 2, functioncode=4
            )
            value = (raw_value[0] << 16 | raw_value[1]) * scale_factor
            return value
        except Exception as e:
            self.logger.error(f"Error reading register {register_address}: {e}")
            return None
        
    async def read_register(self, register_address, number_of_decimals, signed):
        """Read a single register value asynchronously."""
        try:
            return await asyncio.to_thread(
                self.instrument.read_register,
                register_address,
                number_of_decimals,
                signed=signed,
                functioncode=4
        )
        except Exception as e:
            self.logger.error(f"Error reading register {register_address}: {e}")
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
            self.logger.error(f"Error reading data: {e}")
            return None

# -------------------------------------------------------------------------------
# SQL Data Manager Module
# -------------------------------------------------------------------------------
class SQLDataManager:
    def __init__(self, db_config, logger: logging.Logger):
        self.db_config = db_config
        self.logger = logger
        
    async def save_to_sql(self, data_buffer):
        insert_query = """
        INSERT INTO Office_Readings 
           (Timestamp, VoltageL1_v, VoltageL2_v, VoltageL3_v, 
            VoltageL12_v, VoltageL23_v, VoltageL31_v,
            VoltageL12_maxv, VoltageL23_maxv, VoltageL31_maxv,
            VoltageL12_minv, VoltageL23_minv, VoltageL31_minv,
            CurrentL1_I, CurrentL2_I, CurrentL3_I, CurrentLn_I,
            TotalRealPower, TotalApparentPower, TotalReactivePower,
            TotalPowerFactor, Frequency,
            TotalRealEnergy, TotalReactiveEnergy, TotalApparentEnergy)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            conn = await asyncio.to_thread(pymssql.connect, **self.db_config)
            cursor = conn.cursor()
            rows = []
            for data in data_buffer:
                rows.append((
                    data['timestamp'],
                    data.get('voltage_l1'),
                    data.get('voltage_l2'),
                    data.get('voltage_l3'),
                    data.get('voltage_l12'),
                    data.get('voltage_l23'),
                    data.get('voltage_l31'),
                    data.get('voltage_l12_max'),
                    data.get('voltage_l23_max'),
                    data.get('voltage_l31_max'),
                    data.get('voltage_l12_min'),
                    data.get('voltage_l23_min'),
                    data.get('voltage_l31_min'),
                    data.get('current_l1'),
                    data.get('current_l2'),
                    data.get('current_l3'),
                    data.get('current_ln'),
                    data.get('total_real_power'),
                    data.get('total_apparent_power'),
                    data.get('total_reactive_power'),
                    data.get('total_power_factor'),
                    data.get('frequency'),
                    data.get('total_real_energy'),
                    data.get('total_reactive_energy'),
                    data.get('total_apparent_energy')
                ))
            await asyncio.to_thread(cursor.executemany, insert_query, rows)
            await asyncio.to_thread(conn.commit)
            self.logger.info(f"Inserted {len(data_buffer)} SQL records.")
        except Exception as e:
            self.logger.error(f"SQL insert error: {e}")
            if 'conn' in locals():
                await asyncio.to_thread(conn.rollback)
        finally:
            if 'cursor' in locals():
                await asyncio.to_thread(cursor.close)
            if 'conn' in locals():
                await asyncio.to_thread(conn.close)

# -------------------------------------------------------------------------------
# CSV Data Manager Module
# -------------------------------------------------------------------------------
class CSVDataManager:
    def __init__(self, csv_config, logger: logging.Logger):
        self.folder_path = Path(csv_config.get("log_folder", "."))
        self.folder_path.mkdir(parents=True, exist_ok=True)
        self.logger = logger
        
    def get_filename(self, extension="csv"):
        today = datetime.now().strftime("%Y-%m-%d")
        return self.folder_path / f"rx380_data_{today}.{extension}"
        
    async def save_to_csv(self, data):
        filename = self.get_filename()
        file_exists = filename.is_file()
        async with aiofiles.open(filename, 'a', newline='') as csvfile:
            if not file_exists:
                header = ['timestamp'] + list(data.keys())
                header = list(dict.fromkeys(header))  # Remove duplicates if any
                await csvfile.write(','.join(header) + '\n')
            # Construct the row based on the header
            header = ['timestamp'] + list(data.keys())
            header = list(dict.fromkeys(header))
            row = ','.join(str(data.get(field, "")) for field in header) + '\n'
            await csvfile.write(row)
        self.logger.info(f"Data written to CSV: {filename}")

# -------------------------------------------------------------------------------
# Main Application Loop
# -------------------------------------------------------------------------------
async def main():
    modbus_client = ModbusClient(config["modbus"], logger)
    sql_manager = SQLDataManager(config["database"], logger)
    csv_manager = CSVDataManager(config["csv"], logger)
    
    data_interval = config.get("data_save_interval", {}).get("minutes", 10)
    next_save_time = compute_next_save_time(datetime.now(), data_interval)
    logger.info(f"Next data save scheduled at {next_save_time}")
    
    while True:
        now = datetime.now()
        wait_time = (next_save_time - now).total_seconds()
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        data = await modbus_client.read_data()
        if data:
            data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info("Data successfully read from Modbus.")
            await asyncio.gather(
                sql_manager.save_to_sql([data]),
                csv_manager.save_to_csv(data)
            )
            logger.info(f"Data saved at {data['timestamp']}")
        next_save_time = compute_next_save_time(datetime.now(), data_interval)
        logger.info(f"Next data save scheduled at {next_save_time}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
