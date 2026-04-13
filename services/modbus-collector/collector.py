"""
Modbus Collector — polls 4 virtual meters, publishes JSON to MQTT.

Topic format: factory/meter/{meter_id}/readings
Payload: JSON with all electrical measurements + timestamp
"""

import asyncio
import json
import logging
import os
import time

import paho.mqtt.client as mqtt
from pymodbus.client import AsyncModbusTcpClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

METER_HOST    = os.getenv("METER_HOST", "meter-simulator")
MQTT_HOST     = os.getenv("MQTT_HOST", "mqtt-broker")
MQTT_PORT     = int(os.getenv("MQTT_PORT", "1883"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))

METERS = [
    {"id": "METER-01", "zone": "Press Line A",    "port": 5020},
    {"id": "METER-02", "zone": "Press Line B",    "port": 5021},
    {"id": "METER-03", "zone": "CNC & Machining", "port": 5022},
    {"id": "METER-04", "zone": "Assembly & HVAC", "port": 5023},
]


def read_32bit(registers, offset: int) -> int:
    return (registers[offset] << 16) | registers[offset + 1]


async def poll_meter(client: AsyncModbusTcpClient, meter: dict) -> dict | None:
    try:
        # Read 40 registers starting at 4002 (covers all channels up to 4042)
        result = await client.read_input_registers(address=4002, count=40, slave=1)
        if result.isError():
            log.error(f"[{meter['id']}] Modbus read error")
            return None

        r = result.registers
        base = 4002

        def val(addr):
            idx = addr - base
            return read_32bit(r, idx)

        def val16(addr):
            return r[addr - base]

        return {
            "meter_id":           meter["id"],
            "zone":               meter["zone"],
            "timestamp":          time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total_real_energy_kwh":   val(4002),
            "total_real_power_w":      val(4012),
            "total_apparent_power_va": val(4014),
            "total_reactive_power_var":val(4016),
            "power_factor":            val16(4018) / 1000.0,
            "frequency_hz":            val16(4019) / 100.0,
            "current_l1_a":            val(4020) / 1000.0,
            "current_l2_a":            val(4022) / 1000.0,
            "current_l3_a":            val(4024) / 1000.0,
            "voltage_l12_v":           val(4028) / 10.0,
            "voltage_l23_v":           val(4030) / 10.0,
            "voltage_l31_v":           val(4032) / 10.0,
            "voltage_l1_v":            val(4034) / 10.0,
            "voltage_l2_v":            val(4036) / 10.0,
            "voltage_l3_v":            val(4038) / 10.0,
        }
    except Exception as e:
        log.error(f"[{meter['id']}] Exception: {e}")
        return None


async def main():
    mq = mqtt.Client(client_id="modbus-collector")
    mq.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    mq.loop_start()
    log.info(f"Connected to MQTT at {MQTT_HOST}:{MQTT_PORT}")

    clients = {}
    for meter in METERS:
        c = AsyncModbusTcpClient(METER_HOST, port=meter["port"])
        await c.connect()
        clients[meter["id"]] = c
        log.info(f"Connected to {meter['id']} ({meter['zone']}) on port {meter['port']}")

    while True:
        for meter in METERS:
            client = clients[meter["id"]]
            data = await poll_meter(client, meter)
            if data:
                topic = f"factory/meter/{meter['id']}/readings"
                mq.publish(topic, json.dumps(data), qos=1)
                log.info(f"Published {meter['id']}: {data['total_real_power_w']} W, PF={data['power_factor']:.3f}")
        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
