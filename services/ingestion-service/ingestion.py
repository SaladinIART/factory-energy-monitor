"""
Ingestion Service — subscribes to MQTT, writes to TimescaleDB.

Also seeds 3 months of historical data (Jan–Mar 2026) on first startup,
enabling the cost trend and forecast dashboards to have baseline data.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
import random

import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "factory_energy")
DB_USER = os.getenv("DB_USER", "energy_user")
DB_PASS = os.getenv("DB_PASSWORD", "energy_pass")

MQTT_HOST = os.getenv("MQTT_HOST", "mqtt-broker")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

# Malaysian grid carbon intensity factor (kg CO2 per kWh)
CARBON_FACTOR = 0.585

# TNB industrial tariff (sen per kWh) — effective July 2025
TNB_RATE_SEN = 45.40

ZONES = {
    "METER-01": {"name": "Press Line A",    "base_kw": 20.0},
    "METER-02": {"name": "Press Line B",    "base_kw": 14.0},
    "METER-03": {"name": "CNC & Machining", "base_kw": 21.0},
    "METER-04": {"name": "Assembly & HVAC", "base_kw": 16.0},
}


def get_conn():
    for attempt in range(30):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASS
            )
            log.info("Connected to TimescaleDB")
            return conn
        except Exception as e:
            log.warning(f"DB not ready ({attempt+1}/30): {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to TimescaleDB after 30 attempts")


def seed_historical(conn):
    """Insert 3 months of synthetic historical readings if table is empty."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM meter_readings WHERE time < NOW() - INTERVAL '1 day'")
    if cur.fetchone()[0] > 0:
        log.info("Historical data already seeded — skipping")
        cur.close()
        return

    log.info("Seeding 3 months of historical data (Jan–Mar 2026)...")

    rows = []
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end   = datetime(2026, 4, 1, tzinfo=timezone.utc)
    tick  = start

    while tick < end:
        hour = tick.hour + tick.minute / 60.0
        # Simplified load profile
        if tick.hour < 7 or tick.hour >= 18:
            lf = 0.12
        elif 12 <= tick.hour < 13:
            lf = 0.55
        else:
            lf = 0.88 + random.uniform(-0.05, 0.05)

        # CNY shutdown: 28 Jan – 5 Feb 2026
        if datetime(2026, 1, 28, tzinfo=timezone.utc) <= tick <= datetime(2026, 2, 5, tzinfo=timezone.utc):
            lf = 0.05

        for meter_id, zone in ZONES.items():
            kw = zone["base_kw"] * lf * random.uniform(0.95, 1.05)
            pf = random.uniform(0.82, 0.95)
            v  = random.uniform(228, 235)
            rows.append((
                tick,
                meter_id,
                zone["name"],
                round(kw * 1000),           # W
                round(kw * 1000 / pf),      # VA
                round(pf, 3),
                round(random.uniform(49.8, 50.2), 2),
                round(v, 1),
                round(kw / 3 * 1000 / (v * pf), 2),  # A per phase (approx)
                round(kw * (10 / 60), 3),   # kWh in 10-min interval
            ))

        tick += timedelta(minutes=10)

    execute_values(cur, """
        INSERT INTO meter_readings
          (time, meter_id, zone_name, real_power_w, apparent_power_va,
           power_factor, frequency_hz, voltage_l1_v, current_l1_a, energy_kwh)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, rows, page_size=1000)
    conn.commit()
    cur.close()
    log.info(f"Seeded {len(rows)} historical rows")


def on_message(client, userdata, msg):
    conn = userdata["conn"]
    try:
        data = json.loads(msg.payload)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO meter_readings
              (time, meter_id, zone_name, real_power_w, apparent_power_va,
               power_factor, frequency_hz, voltage_l1_v, current_l1_a, energy_kwh)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data["timestamp"],
            data["meter_id"],
            data["zone"],
            data["total_real_power_w"],
            data["total_apparent_power_va"],
            data["power_factor"],
            data["frequency_hz"],
            data["voltage_l1_v"],
            data["current_l1_a"],
            data["total_real_energy_kwh"],
        ))
        conn.commit()
        cur.close()
    except Exception as e:
        log.error(f"Insert error: {e}")
        conn.rollback()


def main():
    conn = get_conn()
    seed_historical(conn)

    mq = mqtt.Client(client_id="ingestion-service", userdata={"conn": conn})
    mq.on_message = on_message
    mq.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    mq.subscribe("factory/meter/+/readings", qos=1)
    log.info("Subscribed to factory/meter/+/readings — waiting for data...")
    mq.loop_forever()


if __name__ == "__main__":
    main()
