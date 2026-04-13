"""
Meter Simulator — 4 virtual RX380 Modbus TCP meters

Simulates realistic load profiles for a Malaysian aluminium/manufacturing SME:
  METER-01: Press Line A    (~8 machines, ~2.5 kW avg)
  METER-02: Press Line B    (~7 machines, ~2.0 kW avg)
  METER-03: CNC & Machining (~6 machines, ~3.5 kW avg)
  METER-04: Assembly & HVAC (~4 machines + HVAC, ~4.0 kW avg)

Each meter listens on a separate TCP port (5020–5023).
Register map mirrors the RX380 (Modbus function code 4, 32-bit scaled values).
"""

import asyncio
import math
import random
import time
from pymodbus.server import StartAsyncTcpServer
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext

# ── Zone definitions ──────────────────────────────────────────────────────────
ZONES = [
    {"name": "Press Line A",    "base_kw": 20.0,  "port": 5020},
    {"name": "Press Line B",    "base_kw": 14.0,  "port": 5021},
    {"name": "CNC & Machining", "base_kw": 21.0,  "port": 5022},
    {"name": "Assembly & HVAC", "base_kw": 16.0,  "port": 5023},
]

# ── Modbus register addresses (matching RX380 register map) ───────────────────
REG_REAL_ENERGY    = 4002   # kWh accumulator (32-bit, scale 1)
REG_REAL_POWER     = 4012   # Active power W  (32-bit, scale 1)
REG_APPARENT_POWER = 4014   # VA              (32-bit, scale 1)
REG_REACTIVE_POWER = 4016   # VAR             (32-bit, scale 1)
REG_POWER_FACTOR   = 4018   # PF × 1000       (16-bit)
REG_FREQUENCY      = 4019   # Hz × 100        (16-bit)
REG_CURRENT_L1     = 4020   # A × 1000        (32-bit, scale 0.001)
REG_CURRENT_L2     = 4022
REG_CURRENT_L3     = 4024
REG_VOLTAGE_L12    = 4028   # V × 10          (32-bit, scale 0.1)
REG_VOLTAGE_L23    = 4030
REG_VOLTAGE_L31    = 4032
REG_VOLTAGE_L1     = 4034   # V × 10 (phase)  (32-bit, scale 0.1)
REG_VOLTAGE_L2     = 4036
REG_VOLTAGE_L3     = 4038


def load_factor(hour: float) -> float:
    """Realistic factory load profile: ramp up 7am, lunch dip, ramp down 5pm."""
    if hour < 6.5:
        return 0.12   # overnight standby
    elif hour < 7.5:
        return 0.12 + 0.78 * (hour - 6.5)   # morning ramp-up
    elif hour < 12.0:
        return 0.90 + random.uniform(-0.05, 0.05)
    elif hour < 13.0:
        return 0.55 + random.uniform(-0.05, 0.05)   # lunch break
    elif hour < 17.0:
        return 0.88 + random.uniform(-0.06, 0.06)
    elif hour < 18.0:
        return 0.88 - 0.76 * (hour - 17.0)   # wind-down
    else:
        return 0.12


def write_32bit(store, reg: int, value: int):
    """Write a 32-bit value as two consecutive 16-bit Modbus registers."""
    high = (value >> 16) & 0xFFFF
    low  = value & 0xFFFF
    store.setValues(4, reg, [high, low])


def update_registers(store, zone: dict, energy_kwh: float):
    """Recalculate and write all registers for one meter."""
    now   = time.localtime()
    hour  = now.tm_hour + now.tm_min / 60.0
    lf    = load_factor(hour)

    kw    = zone["base_kw"] * lf
    pf    = round(random.uniform(0.82, 0.95), 3)
    freq  = round(random.uniform(49.8, 50.2), 2)
    v_ln  = round(random.uniform(228.0, 235.0), 1)  # line-to-neutral
    v_ll  = round(v_ln * math.sqrt(3), 1)            # line-to-line
    i_per = round(kw * 1000 / (3 * v_ln * pf), 2)   # per-phase current

    w   = int(kw * 1000)
    va  = int(w / pf)
    var = int(math.sqrt(max(va**2 - w**2, 0)))

    write_32bit(store, REG_REAL_ENERGY,    int(energy_kwh))
    write_32bit(store, REG_REAL_POWER,     w)
    write_32bit(store, REG_APPARENT_POWER, va)
    write_32bit(store, REG_REACTIVE_POWER, var)
    store.setValues(4, REG_POWER_FACTOR, [int(pf * 1000)])
    store.setValues(4, REG_FREQUENCY,    [int(freq * 100)])

    for reg in (REG_CURRENT_L1, REG_CURRENT_L2, REG_CURRENT_L3):
        write_32bit(store, reg, int(i_per * 1000))

    for reg in (REG_VOLTAGE_L12, REG_VOLTAGE_L23, REG_VOLTAGE_L31):
        write_32bit(store, reg, int(v_ll * 10))

    for reg in (REG_VOLTAGE_L1, REG_VOLTAGE_L2, REG_VOLTAGE_L3):
        write_32bit(store, reg, int(v_ln * 10))


async def run_meter(zone: dict):
    """Run one virtual meter: update registers every 10 s, serve over Modbus TCP."""
    store = ModbusSequentialDataBlock(0, [0] * 5000)
    context = ModbusServerContext(
        slaves=ModbusSlaveContext(ir=store),
        single=True
    )

    energy_kwh = 0.0
    last_tick  = time.time()

    async def tick():
        nonlocal energy_kwh, last_tick
        while True:
            now     = time.time()
            elapsed = now - last_tick
            last_tick = now

            # Accumulate energy from instantaneous kW reading
            kw = zone["base_kw"] * load_factor(
                time.localtime().tm_hour + time.localtime().tm_min / 60.0
            )
            energy_kwh += kw * (elapsed / 3600.0)

            update_registers(store, zone, energy_kwh)
            await asyncio.sleep(10)

    asyncio.create_task(tick())
    print(f"[{zone['name']}] Modbus TCP server on port {zone['port']}")
    await StartAsyncTcpServer(context, address=("0.0.0.0", zone["port"]))


async def main():
    await asyncio.gather(*[run_meter(z) for z in ZONES])


if __name__ == "__main__":
    asyncio.run(main())
