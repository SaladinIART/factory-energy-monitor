# Factory Energy Monitor

> A real-world energy monitoring system built for a Malaysian manufacturing SME —
> deployed across 25 machine locations and adopted as the department standard for
> monthly production-cost reporting.

---

## Why This Was Built

A mid-sized aluminium manufacturer in Malaysia had a problem every production manager recognises: electricity bills of **RM30,000–65,000 per month** with no breakdown by machine or zone. They knew costs were rising, but couldn't say where the money was going.

The factory manager tasked a 2-person digitization team with two things:
1. Build a working proof-of-concept for energy monitoring
2. Use it to inform which contractor to hire for the full rollout

This system was that proof-of-concept. It worked. It was used by management for monthly cost reporting. It became the department's standard framework for machine data collection — and later the foundation for a broader IIoT telemetry platform.

**The timing matters too.** Malaysia's [Energy Efficiency and Conservation Act (EECA 2024)](https://www.st.gov.my/en/web/guest/content/eeca) took effect on 1 January 2025. Factories consuming more than 21,600 GJ per year must now appoint a Registered Energy Manager, conduct annual energy audits, and face fines of RM20,000–100,000 for non-compliance. A carbon tax of RM15/tCO₂e applies from 2026. A system like this isn't just useful — it's becoming a legal requirement.

---

## What It Does

- Reads electrical data (voltage, current, power, energy, power factor) from **RX380 Modbus power meters** at each machine zone
- Stores readings to a time-series database every 10 minutes
- Provides a live dashboard showing **real-time power consumption per zone**
- Calculates **RM cost and CO₂ emissions** based on current TNB tariff rates
- Forecasts **next-month energy cost** from 3 months of historical data

---

## Production Deployment (Sanitised)

This system ran on industrial hardware at a manufacturing plant floor:

| Component | Details |
|-----------|---------|
| Controller | IRIV Cytron industrial PC (ruggedised, 24V DC) |
| Power meters | RX380 Modbus RTU (25 locations across plant floor) |
| Protocol | Modbus RTU over RS-485, bridged to TCP |
| Message broker | RabbitMQ / MQTT |
| Database | Microsoft SQL Server |
| Dashboard | Internal web interface |

All site-specific configuration (IP addresses, credentials, database names, file paths) has been removed from this repository. See `.env.example` for the configuration structure.

---

## This Repository — Open-Source Version

Because the production stack depends on licensed software and site-specific hardware, this repository ships a **fully open-source Docker equivalent** that anyone can run locally. It simulates 4 production zones with realistic Malaysian factory load profiles.

```
docker compose up
```

Then open **http://localhost:3000** — Grafana loads with 3 pre-built dashboards and 3 months of seeded historical data.

### Simulated Factory Layout

| Meter | Zone | Machines | Avg Load | ~kWh/month |
|-------|------|----------|----------|------------|
| METER-01 | Press Line A | 8 machines | 20 kW | ~3,520 kWh |
| METER-02 | Press Line B | 7 machines | 14 kW | ~2,464 kWh |
| METER-03 | CNC & Machining | 6 machines | 21 kW | ~3,696 kWh |
| METER-04 | Assembly & HVAC | 4 machines + HVAC | 16 kW | ~2,816 kWh |
| **Total** | | **25 machines** | | **~75,000 kWh/month** |

At TNB's current industrial rate (45.40 sen/kWh), that's approximately **RM34,000–42,000/month**.

Load profiles follow a real factory pattern: ramp-up at 7am, lunch dip at noon, wind-down at 5pm.

---

## Architecture

```
RX380 Power Meters (Modbus RTU/TCP)
         │
         ▼
  [meter-simulator]        ← 4 virtual meters, Modbus TCP (ports 5020–5023)
         │
         ▼
  [modbus-collector]       ← polls all meters every 10s, publishes JSON to MQTT
         │
         ▼
   [mqtt-broker]           ← eclipse-mosquitto
         │
         ▼
  [ingestion-service]      ← subscribes MQTT → writes to TimescaleDB
         │                    (also seeds 3 months of historical data on startup)
         ▼
   [timescaledb]           ← time-series storage (PostgreSQL + TimescaleDB)
         │
         ▼
     [grafana]             ← 3 pre-provisioned dashboards
```

**Production stack used:** Modbus RTU → RabbitMQ → MSSQL → internal dashboard.
**This repo uses:** Modbus TCP → Mosquitto MQTT → TimescaleDB → Grafana. Fully open-source, no licences required.

---

## Dashboards

| Dashboard | What It Shows |
|-----------|--------------|
| **Live Plant Overview** | Real-time kW per zone, power factor gauges (alert <0.85), total plant load |
| **Cost & Consumption** | RM cost and kWh by zone (today/month), CO₂ estimate, 3-month cost trend |
| **Next-Month Forecast** | Projected kWh, RM cost, CO₂ per zone — based on 3-month rolling average |

---

## Malaysian Regulatory Context

Relevant for engineers working in Malaysian manufacturing:

- **EECA 2024** (enforced 1 Jan 2025): factories consuming >21,600 GJ/year must appoint a Registered Energy Manager and conduct annual energy audits. Penalties: RM20,000–100,000.
- **TNB MD tariff**: 45.40 sen/kWh base rate (13.6% increase effective July 2025). Demand charges apply separately.
- **Carbon tax**: RM15/tCO₂e from 2026 — Scope 1 & 2 emissions tracking required.
- **Malaysia grid carbon factor**: 0.585 kg CO₂/kWh (used in CO₂ estimates).

---

## How to Run

**Requirements:** Docker Desktop (Windows/Mac/Linux)

```bash
git clone https://github.com/SaladinIART/factory-energy-monitor
cd factory-energy-monitor
docker compose up
```

Open http://localhost:3000 → login: `admin / admin` → navigate to *Factory Energy Monitor* folder.

All 6 services start automatically. Historical data seeds on first run (takes ~30 seconds).

---

## Lessons for Engineers in Similar Situations

This was built by a 2-person team, alongside other production projects, with no prior formal IIoT training. A few things that proved true:

- **Start with one meter and prove the data pipeline.** Scaling to 25 was straightforward once the first one worked.
- **Factory managers respond to cost numbers, not architecture diagrams.** "Zone 3 cost RM8,400 last month" gets attention. "We have a Modbus pipeline" does not.
- **A working system with messy code beats a perfect system never delivered.** The v1.56 code in this repo isn't clean. It ran in production for months.
- **Document as you go.** Even rough notes in the code help the next person.

---

## Version History

| Version | Change |
|---------|--------|
| v1.5 | First working proof-of-concept — single meter, CSV logging |
| v1.56 | Added SQL storage + ODS export, improved error handling |
| v1.61 | Async rewrite, modular structure, multi-meter support |
| v1.62 | Stability fixes, 10-minute data intervals |
| v1.6 (module) | Refactored into `main.py` / `modbus_client.py` / `data_storage.py` |
| Docker | Open-source simulation stack (this repo) |

---

## Evolution of This Work

This system became the foundation for two subsequent projects:

**factory-energy-monitor** → [`edge-telemetry-platform`](https://github.com/SaladinIART/edge-telemetry-platform) → [`IIoT-Telemetry-Stack`](https://github.com/SaladinIART/IIoT-Telemetry-Stack)

Each step generalised the approach: from a single-purpose energy monitor, to a reusable edge data collection framework, to a full multi-protocol IIoT telemetry stack.
