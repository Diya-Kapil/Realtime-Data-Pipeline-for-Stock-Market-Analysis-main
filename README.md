<div align="center">

# TICK × STREAM

### A real-time stock market data pipeline

*Alpha Vantage → Kafka → PostgreSQL — captured as it moves, not after it settles.*

## Why this exists

Markets don't wait for nightly batch jobs. Prices move in minutes — sometimes seconds — and a pipeline that only wakes up at midnight is already late.

**TICK × STREAM** is a lean ETL path that pulls intraday equity bars, pushes them onto a Kafka topic, and lands them in Postgres while the day is still unfolding. No warehouse layer pretending to be real-time. Just a clear line from API to store.

| Role | Tool |
|------|------|
| Source | [Alpha Vantage](https://www.alphavantage.co/) intraday API |
| Bus | Apache Kafka on Confluent Cloud |
| Sink | PostgreSQL on Aiven |
| Glue | Python (`confluent-kafka`, `requests`, `psycopg2`) |

---

## The flow

### 1. Produce

`producer.py` hits Alpha Vantage for 5-minute IBM bars, serializes each candle as JSON, and publishes to `topic_0`. Delivery is confirmed via a callback so dropped messages don't vanish silently.

<img width="1600" height="866" alt="image" src="https://github.com/user-attachments/assets/83ab9661-e139-4523-a9f9-526e73c9e4f6" />

### 2. Stream

Kafka on Confluent Cloud is the spine — it buffers, orders, and fans out messages between producer and consumer without either side knowing the other's schedule.

<img width="1600" height="859" alt="image" src="https://github.com/user-attachments/assets/1cdf61cc-e3d4-4158-8280-d6f497698336" />

### 3. Consume & land

`consumer.py` polls `topic_0`, parses OHLCV fields, and upserts rows into `tbl_messages` on Aiven Postgres. The table is created on first connect if it isn't already there.

<img width="1600" height="866" alt="image" src="https://github.com/user-attachments/assets/da7a5844-76a0-43c7-8e01-b12134bc2f50" />

### 4. Ask questions

Once tick data is in Postgres, SQL does the rest — aggregations, windows, and spot checks against live market movement.

<img width="1600" height="867" alt="image" src="https://github.com/user-attachments/assets/08d459e8-722b-43f4-85a6-70ad45c5e65f" />

---

## Concepts, sharpened

Two distinctions this project is built to make concrete:

### ETL vs ELT

| | **ETL** | **ELT** |
|---|---|---|
| Order | Extract → Transform → Load | Extract → Load → Transform |
| Where transform happens | Before the warehouse | Inside the warehouse |
| This pipeline | Closer to **ETL** — messages are shaped into OHLCV rows as they leave Kafka, then written | Would dump raw JSON into a lake first, then remodel later |

Here, transformation is light (JSON → typed columns) and happens on the consumer path — classic streaming ETL.

### Batch vs stream

| | **Batch** | **Stream** |
|---|---|---|
| Cadence | Fixed windows (hourly, nightly) | Continuous / near-continuous |
| Latency | Minutes to hours | Seconds to minutes |
| This pipeline | Would collect a day's candles, then load once | **Stream** — Kafka keeps the pipe open; the consumer never waits for a full file |

---

## Project layout

```
.
├── producer.py                 # Alpha Vantage → Kafka
├── consumer.py                 # Kafka → PostgreSQL
├── consumerdocumentation.txt   # Consumer design notes
└── README.md
```

---

## Quick start

### You need

- Python 3.8+
- A Confluent Cloud Kafka cluster + topic (`topic_0`)
- An Aiven (or local) PostgreSQL instance
- An [Alpha Vantage](https://www.alphavantage.co/support/#api-key) API key

### Install

```bash
git clone https://github.com/yourusername/Realtime-Data-Pipeline-for-Stock-Market-Analysis.git
cd Realtime-Data-Pipeline-for-Stock-Market-Analysis
pip install confluent-kafka requests psycopg2-binary
```

### Configure

Set your credentials in the producer/consumer configs (or, better, move them to environment variables before sharing the repo):

| Variable idea | Used by |
|---------------|---------|
| `BOOTSTRAP_SERVERS`, `SASL_USERNAME`, `SASL_PASSWORD` | both scripts |
| `ALPHA_VANTAGE_API_KEY`, `SYMBOL` | producer |
| `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` | consumer |

### Run

Terminal A — start publishing:

```bash
python producer.py
```

Terminal B — start landing:

```bash
python consumer.py
```

Watch messages land in `tbl_messages` (`Open`, `High`, `Low`, `Close`, `Volume`).

---

## What you'll walk away knowing

- How to wire a **Kafka producer** that pulls from a live market API
- How a **Kafka consumer** turns topic payloads into durable relational rows
- How managed cloud pieces (Confluent + Aiven) remove local cluster babysitting
- When to choose **stream processing** over a cron-driven batch load

---

## What's next

- Swap demo API limits for a paid key and multi-symbol feeds  
- Add Tableau / Power BI / Metabase on top of Postgres  
- Introduce schema validation (Avro / Schema Registry) before the sink  
- Scale out consumer groups and add dead-letter handling  

---

<div align="center">

*Built as a data engineering exercise in stream processing, ETL, and cloud-native plumbing.*

**TICK × STREAM** — don't wait for the market close.

</div>
