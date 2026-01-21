# Axiom

**Axiom** is a cross-engine data control plane that enforces correctness, governance, and safe state transitions for modern data systems.

Modern data platforms rely on many independent engines — batch, streaming, and interactive — all mutating shared datasets. Axiom provides a single authoritative layer that decides **whether a data mutation is allowed**, and ensures that all changes obey explicit invariants.

---

## Why Axiom?

Today’s data stacks suffer from:
- Silent table corruption
- Unsafe schema evolution
- Conflicting concurrent writes across engines
- Irreversible backfills and rewrites
- Governance that exists only on paper

Axiom addresses this gap by acting as the **control plane for data mutations**, not another compute or storage engine.

---

## What Axiom Is (and Is Not)

### Axiom **is**
- An authoritative metadata and state system
- A coordinator for safe data mutations
- A policy and invariant enforcement layer
- Engine-agnostic and storage-agnostic
- Deterministic and replayable by design

### Axiom **is not**
- A query engine
- A streaming engine
- A data warehouse
- A BI or visualization tool

Axiom integrates with existing systems instead of replacing them.

---

## Core Concepts

### 1. Authoritative Metadata Log
All table mutations are recorded in an append-only, versioned log.
This log is the source of truth for table state.

### 2. Explicit Table State Machine
Tables are modeled as stateful systems with well-defined transitions:
```
CREATED → ACTIVE → MUTATING → ACTIVE
                  ↓
              ROLLING_BACK
```

Illegal or unsafe transitions are rejected.

### 3. Invariants and Policies
Rules that must always hold true, such as:
- No destructive schema changes in production
- No concurrent writers across engines
- No rewrites during active streaming ingestion

Violations are detected *before* data is corrupted.

### 4. Deterministic Replay
Given the metadata log, Axiom can deterministically reconstruct table state at any point in time — enabling auditing, debugging, and recovery.

---

## Architecture (High Level)

```
+---------------------+
|  Data Engines       |
|  (Spark, Flink,     |
|   Trino, etc.)      |
+----------+----------+
           |
           | adapters
           v
+---------------------+
|      Axiom          |
|  Control Plane      |
|  (Rust Kernel)      |
+----------+----------+
           |
           v
+---------------------+
| Object Storage      |
| (S3, GCS, ADLS)     |
+---------------------+
```

Axiom coordinates. Engines execute.

---

## Current Status

**Early development / design phase**

Initial focus:
- Core metadata log
- Table state machine
- Invariant framework
- Apache Iceberg integration

---

## Language Choice

The core Axiom kernel is written in **Rust** to guarantee:
- Strong correctness guarantees
- Explicit state modeling
- Deterministic behavior
- Long-term maintainability

---

## Vision

Over time, Axiom aims to become:
> The neutral, cross-engine authority that data systems trust before mutating state.

---

## License

Apache 2.0
