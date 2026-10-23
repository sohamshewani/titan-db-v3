# titan-db
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/titan-db/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/titan-db/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/titan-db) [![Stars](https://img.shields.io/github/stars/sohamshewani/titan-db?style=flat)](https://github.com/sohamshewani/titan-db)

## Executive Summary & Problem Space

Modern analytical and transactional workloads demand high-throughput data processing coupled with predictable, low-latency tail responses ($p_{99} < 1.5\text{ms}$). Traditional Java database engines frequently suffer from two fundamental bottlenecks: garbage collection (GC) pause overheads introduced by massive object allocations, and severe lock contention during concurrent multiversion reads and writes.

`titan-db` is an enterprise-grade, high-performance embedded storage and execution engine engineered from the ground up in Java to eliminate these failure modes. By leveraging custom off-heap memory management (`OffHeapMemoryAllocator`), vectorized execution pipelines (`VectorizedExecutionEngine`, `ColumnarTable`), SIMD-friendly bit-packing (`RunLengthBitPacker`), succinct compressed indexing (`RoaringBitmapIndex`), and lock-free Multi-Version Concurrency Control (`MVCCManager`), `titan-db` achieves C++-tier hardware utilization while retaining the safety and expressiveness of the JVM.

---

## Structural ASCII Architecture / Dataflow Diagram

```
 +-----------------------------------------------------------------------------------------------+
 |                                     Client / API Layer                                        |
 +-----------------------------------------------------------------------------------------------+
                                                |
                                                v
 +-----------------------------------------------------------------------------------------------+
 |                              MVCC & Transactional Coordinator                                 |
 |                      (MVCCManager: Snapshot Isolation, Read-Write Locks)                      |
 +-----------------------------------------------------------------------------------------------+
             |                                                           |
             v (Write Path)                                              v (Read Path)
 +-----------------------+                                   +-----------------------------------+
 |  ColumnarTable        |                                   |       VectorizedExecutionEngine    |
 |  - Append-only Logs   |                                   |       - Batch-Oriented Pipelines  |
 +-----------------------+                                   +-----------------------------------+
             |                                                           |
             v                                                           v
 +-----------------------------------------------------------------------------------------------+
 |                                  Storage & Indexing Subsystem                                 |
 |  +---------------------------------+   +--------------------------------------------------+   |
 |  | RunLengthBitPacker              |   | RoaringBitmapIndex                               |   |
 |  | (Compressed Bit-Level Storage)  |   | (Succinct Positional Indexing)                   |   |
 |  +---------------------------------+   +--------------------------------------------------+   |
 +-----------------------------------------------------------------------------------------------+
                                                |
                                                v
 +-----------------------------------------------------------------------------------------------+
 |                             Off-Heap Memory & Resource Management                             |
 |                     (OffHeapMemoryAllocator: Unsafe Native Memory / Arena)                    |
 +-----------------------------------------------------------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

### Thread Safety & Lock Contention Prevention
`titan-db` implements a non-blocking multiversion concurrency control (MVCC) protocol coordinated by `MVCCManager`. 
* **Readers** acquire a read snapshot timestamp atomically, allowing lock-free reads over historical columnar versions without blocking concurrent writers.
* **Writers** use fine-grained optimistic concurrency control combined with epoch-based commit queues to serialize modifications to shared columnar tables without cascading lock contention.

### Memory Ownership & Off-Heap Allocations
To eliminate JVM Garbage Collection pauses during intensive scan and ingestion phases, `titan-db` manages its primary working sets outside the Java heap using `OffHeapMemoryAllocator`.
* **Zero-Copy Serialization:** Data layout structures map directly onto native byte buffers.
* **Deterministic Deallocation:** Memory arenas track explicit reference counts and leverage Cleaner mechanisms combined with explicit lifecycle teardowns to prevent native memory leaks.
* **Safety Boundaries:** Bounds checking is enforced at native pointer arithmetic boundaries to trap out-of-bounds accesses immediately.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation / Path | Algorithmic Complexity | Target Latency ($p_{50}$ / $p_{99}$) | Bottleneck / Notes |
| :--- | :--- | :--- | :--- |
| **Point Read** | $O(1)$ amortized | $150\mu\text{s} / 450\mu\text{s}$ | Roaring bitmap lookup + off-heap fetch |
| **Vectorized Scan** | $O(N / B)$ ($B = \text{Vector Size}$) | $400\mu\text{s} / 1.2\text{ms}$ | Memory bandwidth bound |
| **Range Write / Append**| $O(1)$ amortized | $250\mu\text{s} / 800\mu\text{s}$ | Sequential off-heap buffer appends |
| **Bitmap Index Intersect**| $O(\min(N, M))$ | $80\mu\text{s} / 210\mu\text{s}$ | SIMD-accelerated bitwise operations |
| **MVCC Snapshot Commit**| $O(1)$ | $50\mu\text{s} / 150\mu\text{s}$ | Atomic sequence bump & commit log append |

---

## Security Architecture & Threat Model

`titan-db` is designed for deployment in hostile multi-tenant cloud environments and zero-trust enterprise infrastructures.

* **Input Boundaries & Sanitization:** All incoming byte payloads, schema definitions, and query predicates pass through rigorous boundary-validation filters prior to native off-heap deserialization. Malformed inputs throw explicit bounds exceptions, aborting execution before memory access occurs.
* **Memory Corruption Protections:** Direct unsafe pointer manipulations are strictly encapsulated within `OffHeapMemoryAllocator` and protected by explicit length assertions. Buffer overflows are intercepted via runtime offset validation.
* **Threat Vectors & Mitigations:**
  * *Heap Exhaustion / OOM Attacks:* Enforced quotas on native allocation pools restrict unbounded memory consumption by untrusted queries.
  * *Concurrency Race Conditions:* All state transitions within `MVCCManager` utilize volatile state variables and atomic compare-and-swap (CAS) primitives to prevent TOCTOU (Time-of-Check to Time-of-Use) vulnerabilities.

---

## Build, Verification & Benchmarking Recipes

### Prerequisites
* Java Development Kit (JDK) 17+
* Apache Maven 3.8+
* Python 3.9+ (for invariant testing and benchmarking suites)

### 1. Compile and Package the Project
```bash
mvn clean package -DskipTests
```

### 2. Run Verification & Invariant Tests
Execute the Python-based structural and invariant verification suite:
```bash
python3 tests/test_invariants.py
```

### 3. Execute Benchmarks
Run the engine throughput and latency benchmark suites:
```bash
python3 benchmarks/bench_engine.py