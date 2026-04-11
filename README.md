# Real-time Data Pipeline with Kafka, Python & PostgreSQL

**A complete real-time data engineering project simulating e-commerce user behavior.**

## 🎯 Project Overview

This project builds an end-to-end real-time data pipeline:

- **Data Source**: Python script generates random user actions (view, add_to_cart, purchase)
- **Message Queue**: Apache Kafka buffers and decouples data production from consumption
- **Stream Processing**: Python consumer aggregates data in 1-minute tumbling windows
- **Storage**: Aggregated results are written to PostgreSQL
- **Visualization**: Flask dashboard displays real-time trends (optional)

## 🛠️ Tech Stack

| Category | Technology |
|----------|------------|
| Language | Python 3.10+ |
| Message Queue | Apache Kafka |
| Database | PostgreSQL 15 |
| Containerization | Docker, Docker Compose |
| Visualization | Flask, Plotly |

## 🏗️ Architecture
┌─────────────┐ ┌───────┐ ┌───────────────┐ ┌────────────┐
│ Producer │ ──► │ Kafka │ ──► │ Consumer │ ──► │ PostgreSQL │
│ (mock data) │ │ │ │ (1-min window)│ │ │
└─────────────┘ └───────┘ └───────────────┘ └────────────┘
│
▼
┌─────────────┐
│ Flask │
│ Dashboard │
└─────────────┘

text

## 📊 Data Flow

1. **Producer**: Generates 5-10 events/second with schema:
   ```json
   {
     "user_id": 123,
     "product_id": 45,
     "action": "view",
     "timestamp": "2026-04-08T20:11:35.457526"
   }
Kafka: Buffers messages in user-behavior topic

Consumer:

Reads from Kafka

Filters view actions

Aggregates by 1-minute tumbling windows

Writes results to PostgreSQL

PostgreSQL: Stores aggregated results in product_views table

🚀 Quick Start
Prerequisites
Docker Desktop

Python 3.10+

Java 11 (for Kafka)

Step 1: Start Docker Containers

🔴docker-compose up -d

This starts:

Zookeeper (port 2181)
Kafka (port 9092)
PostgreSQL (port 5432)

Step 2: Create Database Table

🔴docker exec -it postgres psql -U postgres -d testdb -c "
🔴CREATE TABLE IF NOT EXISTS product_views (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    product_id INTEGER,
    view_count INTEGER
);
"

Step 3: Run Producer(Generate mock data)

🔴python producer.py
   (crtl+c:Stop)
   
Step 4: Run Consumer(Consume from Kafka and write to PostgreSQL)

🔴python consumer.py

Step 5: Verify Results(Optional)

docker exec -it postgres psql -U postgres -d testdb -c "
SELECT * FROM product_views ORDER BY window_start DESC LIMIT 10;
"

📈 Sample Output
Producer Output
json
{"user_id": 63, "product_id": 34, "action": "view", "timestamp": "2026-04-08T20:17:46.307349"}
{"user_id": 38, "product_id": 41, "action": "purchase", "timestamp": "2026-04-08T20:17:46.621124"}
Consumer Output
text
Start consuming Kafka data...
Data for window 2026-04-08 20:16:00 - 2026-04-08 20:17:00 written
Data for window 2026-04-08 20:17:00 - 2026-04-08 20:18:00 written
PostgreSQL Query Result
window_start	window_end	product_id	view_count
2026-04-08 20:16:00	2026-04-08 20:17:00	1	1
2026-04-08 20:16:00	2026-04-08 20:17:00	38	1
2026-04-08 20:16:00	2026-04-08 20:17:00	46	1
🧠 Key Design Decisions
Decision	Reason
Kafka as message broker	Decouples producer/consumer, handles traffic spikes, provides data persistence
1-minute tumbling window	Balances real-time requirements with computational cost
PostgreSQL for storage	ACID compliance, easy integration, sufficient for small-scale
Docker Compose	Reproducible environment, one-command startup
🔧 Future Improvements
Replace Python consumer with Spark Streaming for higher throughput

Add checkpointing for exactly-once processing

Deploy to cloud (AWS MSK + RDS)

Add Grafana dashboard for monitoring

Implement dead letter queue for failed messages

📁 Project Structure
text
docker-project/
├── docker-compose.yml      # Kafka + Zookeeper + PostgreSQL
├── producer.py             # Mock data generator
├── consumer.py             # Kafka consumer + aggregator
├── app.py                  # Flask dashboard (optional)
├── requirements.txt        # Python dependencies
└── README.md              # This file
👨‍💻 Author
Yunze Shi

LinkedIn: linkedin.com/in/yourprofile

GitHub: github.com/yourusername

MSc in Data Engineering, EFREI Paris
