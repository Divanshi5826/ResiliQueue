# Distributed Task Broker

A scalable, fault-tolerant background job queue built with Node.js, Redis, and MongoDB.

I built this project to understand how large-scale asynchronous systems handle backpressure, worker concurrency, and data persistence without dropping tasks. Rather than just using a pre-built library like BullMQ, I wanted to build the core orchestration logic from scratch using Redis primitives.

**The system simulates processing high-volume financial trades**, evaluating risk and priority before delegating them to independent worker nodes.

## Core Services
1. **API Server (Express):** Receives HTTP POST tasks, calculates priority rules, logs the initial state to MongoDB, and pushes to a Redis `ZSET`.
2. **Redis Memory Store:** Acts as the high-speed queue. Uses `ZRANGE/ZREM` for atomic locking so multiple workers don't pick up the same task.
3. **MongoDB:** The permanent source of truth. Used for audit logging and storing the **Dead Letter Queue (DLQ)** for trades that exhaust all retry attempts.
4. **Worker Nodes:** Independent Node processes that poll Redis. They can be scaled horizontally.

## What I learned building this:
* **Atomic Locks:** How to prevent race conditions when multiple workers poll the same Redis queue simultaneously.
* **Backpressure Algorithms:** Returning HTTP 429 when the queue depth exceeds capacity to prevent memory crashes.
* **Fault Tolerance:** Implementing exponential backoff and retry mechanisms for tasks that fail during internal network simulations.
* **Containerization:** Using `docker-compose` to network the Express API, Worker scripts, Redis, and Mongo instances together cleanly.

---

## How to run locally

You can run this using Docker, or natively with Node.js over multiple terminals.

### Method 1: Docker (Easiest)
Make sure Docker Desktop is running, then just run:
```bash
docker-compose up -d --build
```
This spins up the database cluster, the API, and 2 separate worker instances.

### Method 2: Native Node.js
If you have Redis and MongoDB running locally:

**1. Start the API**
```bash
npm install
node index.js
```
The API and telemetry dashboard will be available at `localhost:3000`.

**2. Start the Workers**
Open new terminals and run:
*(PowerShell)*
```bash
$env:WORKER_ID="Alpha"; node workers/processor.js
$env:WORKER_ID="Beta"; node workers/processor.js
```
*(Windows CMD)*
```bash
set WORKER_ID=Alpha && node workers/processor.js
set WORKER_ID=Beta && node workers/processor.js
```

### Dashboard
Once the system is running, navigate to `http://localhost:3000/dashboard.html` to inject test payloads and watch the worker nodes negotiate and process the trades in real-time.
