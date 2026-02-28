# Production-Ready Incremental Export System with Change Data Capture (CDC)

A fully containerized, production-ready backend system that implements **application-level Change Data Capture (CDC)** using timestamp-based watermarking for efficient large-scale data synchronization.

This project simulates real-world data engineering patterns used to sync operational databases with downstream systems such as:

* Data Warehouses
* Search Indexes
* Analytics Pipelines
* Microservices

---

# 📌 Table of Contents

* [Architecture Overview](#architecture-overview)
* [Key Concepts Implemented](#key-concepts-implemented)
* [System Design](#system-design)
* [Technology Stack](#technology-stack)
* [Database Schema](#database-schema)
* [Export Strategies](#export-strategies)
* [Watermarking Strategy](#watermarking-strategy)
* [Atomicity & Transaction Safety](#atomicity--transaction-safety)
* [Asynchronous Processing](#asynchronous-processing)
* [Setup Instructions](#setup-instructions)
* [API Endpoints](#api-endpoints)
* [Testing & Coverage](#testing--coverage)
* [Scalability Considerations](#scalability-considerations)
* [Design Tradeoffs](#design-tradeoffs)

---

# 🏗 Architecture Overview

```
Client
   │
   ▼
FastAPI REST API
   │
   ▼
Background Worker (ThreadPoolExecutor)
   │
   ▼
PostgreSQL (Users + Watermarks)
   │
   ▼
CSV Export Files (Docker Volume)
```

The system uses:

* Stateless API layer
* Stateful watermark tracking
* Transactional export processing
* Background job execution
* Containerized deployment

---

# 🧠 Key Concepts Implemented

### ✅ Change Data Capture (CDC)

Implements application-level CDC using:

* `updated_at` timestamp comparison
* Soft deletes (`is_deleted`)
* Consumer-specific watermark tracking

---

### ✅ Watermarking (High-Water Mark Pattern)

Each consumer has an independent progress tracker stored in the `watermarks` table.

This allows:

* Incremental sync
* Multi-consumer independence
* No duplicate exports
* No data loss

---

### ✅ Atomic Exports

Export + watermark update occur in a **single database transaction**:

```
BEGIN
  Fetch rows
  Write CSV
  Update watermark
COMMIT
```

If export fails → watermark is NOT updated.

Guarantees:

* No partial progress
* No inconsistent state
* Exactly-once semantics per export cycle

---

# 🧱 Technology Stack

| Component        | Technology              |
| ---------------- | ----------------------- |
| Backend          | FastAPI                 |
| Database         | PostgreSQL 13           |
| ORM              | SQLAlchemy              |
| Containerization | Docker + Docker Compose |
| Async Jobs       | ThreadPoolExecutor      |
| Testing          | Pytest                  |
| Data Generation  | Faker                   |

---

# 🗄 Database Schema

## `users` table

| Column     | Type             | Description           |
| ---------- | ---------------- | --------------------- |
| id         | BIGSERIAL        | Primary Key           |
| name       | VARCHAR          | User full name        |
| email      | VARCHAR (UNIQUE) | Email                 |
| created_at | TIMESTAMPTZ      | Creation timestamp    |
| updated_at | TIMESTAMPTZ      | Last update timestamp |
| is_deleted | BOOLEAN          | Soft delete flag      |

Index:

```
CREATE INDEX idx_users_updated_at ON users(updated_at);
```

---

## `watermarks` table

| Column           | Type             | Description                |
| ---------------- | ---------------- | -------------------------- |
| id               | SERIAL           | Primary Key                |
| consumer_id      | VARCHAR (UNIQUE) | Consumer identifier        |
| last_exported_at | TIMESTAMPTZ      | Last exported updated_at   |
| updated_at       | TIMESTAMPTZ      | Watermark update timestamp |

---

# 📤 Export Strategies

## 1️⃣ Full Export

Exports:

```
SELECT * FROM users WHERE is_deleted = FALSE;
```

Used for:

* Initial sync
* Full rebuild

Watermark updated to:

```
MAX(updated_at)
```

---

## 2️⃣ Incremental Export

Exports only:

```
SELECT *
FROM users
WHERE updated_at > last_exported_at
AND is_deleted = FALSE;
```

Used for:

* Ongoing sync
* Efficient large-scale updates

---

## 3️⃣ Delta Export

Exports changed rows with operation metadata:

| Operation | Condition                |
| --------- | ------------------------ |
| INSERT    | created_at == updated_at |
| UPDATE    | updated_at > created_at  |
| DELETE    | is_deleted = TRUE        |

Output format includes `operation` column.

---

# 💧 Watermarking Strategy

Each consumer tracks progress independently:

```
consumer-1 → watermark A
consumer-2 → watermark B
```

This allows:

* Parallel consumers
* Independent synchronization states
* No cross-interference

Watermark is only updated AFTER successful export.

---

# ⚙️ Asynchronous Processing

Exports are long-running operations.

API immediately returns:

```
202 Accepted
```

Export runs in background via:

```python
ThreadPoolExecutor(max_workers=4)
```

Benefits:

* API remains responsive
* No request timeouts
* Scalable job execution

---

# 🔒 Atomicity & Transaction Safety

Export execution flow:

```python
with db.begin():
    rows = fetch_rows(...)
    write_csv(...)
    upsert_watermark(...)
```

If any step fails:

* Entire transaction rolls back
* Watermark is unchanged
* No inconsistent state

---

# 🐳 Containerized Deployment

Single command startup:

```bash
docker-compose up --build
```

Services:

* `app`
* `db`

Features:

* Health checks
* Volume-mounted export directory
* Automatic database initialization
* Idempotent seeding (100,000+ records)

---

# 🧪 Testing & Coverage

Run tests:

```bash
pytest --cov=app --cov-report=term-missing
```

Project maintains:

✅ Minimum 70% coverage
✅ Unit tests for:

* Export logic
* Watermark management
* Delta operation logic

✅ Integration tests for:

* Full export
* Incremental export
* Delta export
* Watermark endpoint

---

# 📡 API Endpoints

## Health Check

```
GET /health
```

Response:

```json
{
  "status": "ok",
  "timestamp": "ISO-8601"
}
```

---

## Full Export

```
POST /exports/full
Header: X-Consumer-ID
```

---

## Incremental Export

```
POST /exports/incremental
Header: X-Consumer-ID
```

---

## Delta Export

```
POST /exports/delta
Header: X-Consumer-ID
```

---

## Get Watermark

```
GET /exports/watermark
Header: X-Consumer-ID
```

---

# 📈 Scalability Considerations

Current implementation:

* Single API service
* In-process background executor

Future production scaling:

* Replace executor with:

  * RabbitMQ
  * Kafka
  * AWS SQS
* Move export worker to separate service
* Horizontal scaling with stateless API nodes

---

# ⚖️ Design Tradeoffs

| Decision              | Tradeoff                              |
| --------------------- | ------------------------------------- |
| Timestamp-based CDC   | Does not capture intermediate changes |
| Application-level CDC | Simpler than log-based tools          |
| CSV export            | Portable but not streaming            |
| ThreadPoolExecutor    | Simple but not distributed            |

---

# 🏆 Production-Readiness Highlights

✔ Idempotent seeding
✔ Indexed updated_at for performance
✔ Transaction-safe watermark updates
✔ Multi-consumer support
✔ Asynchronous long-running job handling
✔ Structured logging
✔ Dockerized deployment
✔ High test coverage

---

# 📌 How to Run

```bash
git clone <repo>
cd project
docker-compose up --build
```

Access Swagger UI:

```
http://localhost:8080/docs
```

---

# 🎯 Final Notes

This project demonstrates:

* Backend system design
* Data engineering CDC patterns
* Stateful stream synchronization
* Transactional integrity
* Scalable architecture principles

It models real-world export pipelines used in modern distributed systems.