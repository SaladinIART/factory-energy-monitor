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

# ------------------------------------------------------------------------------
# 1. External Configuration (embedded for a single-file solution)
# ------------------------------------------------------------------------------

config = {
    "modbus": {
        "port": "/dev/ttyUSB0",         # Change if needed (e.g., '/dev/ttySC0')
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
    }
}

# ------------------------------------------------------------------------------
# 2. Logger Setup (dependency injected into all components)
# ------------------------------------------------------------------------------

def setup_logger(cfg):
    level = getattr(logging, cfg.get("level", "INFO").upper(), logging.INFO)
    logging.basicConfig(
        filename=cfg.get("log_file", "app.log"),
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    # Also output to console:
    console = logging.StreamHandler()
    console.setLevel(level)
    logging.getLogger('').addHandler(console)
    return logging.getLogger()

logger = setup_logger(config["logging"])
logger.info("Logger initialized.")

# ------------------------------------------------------------------------------
# 3. Modbus Client Module (with asynchronous read and dependency injection)
# ------------------------------------------------------------------------------

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
            # Offload blocking call to a thread
            raw_value = await asyncio.to_thread(
                self.instrument.read_registers, register_address, 2, functioncode=4
            )
            value = (raw_value[0] << 16 | raw_value[1]) * scale_factor
            return value
        except Exception as e:
            self.logger.error(f"Error reading register {register_address}: {e}")
            return None

    async def read_data(self):
        data = {}
        try:
            # Example: Read three registers concurrently
            results = await asyncio.gather(
                self.read_scaled_value(4034, 0.1),  # Voltage L1
                self.read_scaled_value(4036, 0.1),  # Voltage L2
                self.read_scaled_value(4038, 0.1)   # Voltage L3
            )
            data['voltage_l1'], data['voltage_l2'], data['voltage_l3'] = results
            self.logger.info("Modbus data read successfully.")
            return data
        except Exception as e:
            self.logger.error(f"Error in read_data: {e}")
            return None

# ------------------------------------------------------------------------------
# 4. Data Storage Module (SQL and CSV storage)
# ------------------------------------------------------------------------------

class SQLDataManager:
    def __init__(self, db_config, logger: logging.Logger):
        self.db_config = db_config
        self.logger = logger
        
    async def save_to_sql(self, data_buffer):
        insert_query = """
        INSERT INTO Office_Readings 
           (Timestamp, VoltageL1_v, VoltageL2_v, VoltageL3_v)
        VALUES (%s, %s, %s, %s)
        """
        try:
            conn = await asyncio.to_thread(pymssql.connect, **self.db_config)
            cursor = conn.cursor()
            rows = []
            for data in data_buffer:
                rows.append((
                    data['timestamp'],
                    data['voltage_l1'],
                    data['voltage_l2'],
                    data['voltage_l3']
                ))
            # Batch insert all rows
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
        # Use asynchronous file I/O with aiofiles
        async with aiofiles.open(filename, 'a', newline='') as csvfile:
            # Write header if file does not exist
            if not file_exists:
                header = ['timestamp'] + list(data.keys())
                # Remove duplicates (timestamp is included twice if present)
                header = list(dict.fromkeys(header))
                await csvfile.write(','.join(header) + '\n')
            # Construct row based on header order
            row = ','.join(str(data.get(field, "")) for field in header) + '\n'
            await csvfile.write(row)
        self.logger.info(f"Data written to CSV: {filename}")

# ------------------------------------------------------------------------------
# 5. Main Application Loop (orchestrator using dependency injection)
# ------------------------------------------------------------------------------

async def main():
    # Create instances of the modules by injecting configuration and logger.
    modbus_client = ModbusClient(config["modbus"], logger)
    sql_manager = SQLDataManager(config["database"], logger)
    csv_manager = CSVDataManager(config["csv"], logger)
    
    logger.info("Entering main loop.")
    # Schedule the next data save every 10 minutes (aligned to the minute)
    next_save_time = (datetime.now() + timedelta(minutes=10)).replace(second=0, microsecond=0)
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
            # Save to both SQL and CSV concurrently
            await asyncio.gather(
                sql_manager.save_to_sql([data]),
                csv_manager.save_to_csv(data)
            )
            logger.info(f"Data saved at {data['timestamp']}")
        # Schedule the next save
        next_save_time = (datetime.now() + timedelta(minutes=10)).replace(second=0, microsecond=0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
