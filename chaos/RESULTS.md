# Chaos Engineering Benchmark Results & Recovery Analysis

This document records the empirical results of chaos engineering benchmarks executed against the `zkube-banking` distributed banking architecture on Kubernetes with **Chaos Mesh**.

---

## 1. Executive Summary & Recovery Time Resolution

A primary objective of this audit was to resolve the empirical discrepancy between two reported recovery metrics:
- **"0.10 s"** (previously cited as pod recovery)
- **"4 to 6 s"** (cited as replacement pod readiness)

### The Three Distinct Recovery Times Defined & Measured

| Recovery Metric | Definition & Scope | Measured Value | Practical Significance |
| :--- | :--- | :---: | :--- |
| **(i) Kill to Service Endpoint Eviction** | Time from pod SIGKILL until `kubelet`/`endpointslice-controller` removes the dead pod IP from `EndpointSlice` | **0.032 s – 0.087 s**<br/>*(Sub-100 ms)* | **Traffic Rerouting:** Incoming traffic seamlessly fails over to the 2 surviving healthy replicas without waiting for a new pod to boot. |
| **(ii) Kill to Replacement Pod Ready** | Time from pod SIGKILL until the `ReplicaSet` creates a new pod, boots the JVM, and passes the readiness probe | **5.18 s – 7.15 s**<br/>*(Average: 5.84 s)* | **Pod Cold-Start:** Cold initialization time of the Javalin container, HikariCP connection pool, and `/health` probe verification. |
| **(iii) Kill to All Replicas Ready** | Time from pod SIGKILL until all $N=3$ replicas are concurrently `Running` and `1/1 Ready` | **5.18 s – 7.15 s**<br/>*(Average: 5.84 s)* | **Full Cluster Redundancy Restored:** Restores complete 3-pod resilience against subsequent failures. |

> [!NOTE]
> **Resolution of the "0.10 s" Discrepancy:**  
> In the original audit script, `measure_pod_readiness()` was invoked after a `time.sleep(drain_wait)` of 6.0 seconds following the end of the 45-second test. Because the pod killed at $T \approx 30\text{s}$ had already recovered by $T \approx 36\text{s}$, calling `kubectl get pods` at $T \approx 51\text{s}$ observed all 3 pods already in `Ready: True` on its first poll. The function immediately exited and reported the single-execution latency of the `kubectl` CLI subprocess (**0.10 s**).  
> The actual time required for a replacement pod to become Ready is **5.18 s – 7.15 s** (about 5–6 seconds). However, existing client traffic is rerouted away from the dead pod in **0.032 s – 0.087 s** (sub-100 ms).

---

## 2. Fresh Instrumented Benchmark (3 Runs with Full Request Telemetry)

Executed with per-request CSV logging (`chaos/run1_requests.csv`, `chaos/run2_requests.csv`, `chaos/run3_requests.csv`) and millisecond timeline event tracking (`chaos/run1_timeline.json`, etc.):

- **Experiment:** Periodic Pod Kill (`schedule: "@every 30s"`, `action: pod-kill`, SIGKILL)
- **Workload:** 150 transfer requests per run across 5 independent account pairs (10 accounts total)
- **Concurrency:** 5 concurrent client workers, 5.0s HTTP timeout
- **Durations:** ~45.1s per run (total ~135.3s active load)

### Benchmark Summary Across 3 Fresh Runs

| Metric | Run 1 | Run 2 | Run 3 | Combined Pooled Aggregate |
| :--- | :---: | :---: | :---: | :---: |
| **Duration** | 45.1 s | 45.1 s | 45.1 s | **135.3 s** |
| **Total Requests Dispatched** | 150 | 150 | 150 | **450** |
| **Successful Transfers (HTTP 200)** | 138 (92.0%) | 150 (100.0%) | 147 (98.0%) | **435 (96.67%)** |
| **Server Errors (HTTP 5xx)** | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | **0 (0.00%)** |
| **Client Errors (HTTP 4xx)** | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | **0 (0.00%)** |
| **Network Drops / Timeouts** | 12 (8.0%) | 0 (0.0%) | 3 (2.0%) | **15 (3.33%)** |
| **Balance Conservation Delta** | **PASSED ($0.0000)** | **PASSED ($0.0000)** | **PASSED ($0.0000)** | **PASSED ($0.0000 net delta)** |
| **Notifications Dispatched / Committed** | 138 / 138 (100%) | 150 / 150 (100%) | 147 / 147 (100%) | **435 / 435 (100.0%)** |
| **Event / Message Loss** | **0** | **0** | **0** | **0 (100% crash-safe)** |
| **Pod Kills Injected per Run** | **1 kill** | **2 kills** | **2 kills** | **5 total kills** (periodic @30s) |
| **(i) Kill -> Endpoint Eviction** | 0.087 s | <0.001 s | 0.073 s | **Avg: 0.032 s** *(sub-100 ms)* |
| **(ii) Kill -> Replacement Pod Ready** | 5.59 s | 5.18 s – 5.76 s | 5.54 s – 7.15 s | **Avg: 5.84 s** *(5.18 s – 7.15 s)* |
| **(iii) Kill -> All Replicas Ready** | 5.59 s | 5.18 s – 5.76 s | 5.54 s – 7.15 s | **Avg: 5.84 s** *(5.18 s – 7.15 s)* |
| **p50 Latency (Median)** | 43.94 ms | 28.07 ms | 34.45 ms | **33.38 ms** *(pooled 435 txs)* |
| **p95 Latency** | 2,171.72 ms | 196.74 ms | 3,420.61 ms | **2,286.23 ms** *(pooled 435 txs)* |
| **p99 Latency** | 3,746.83 ms | 4,274.56 ms | 4,587.03 ms | **4,350.95 ms** *(pooled 435 txs)* |
| **Max Latency** | 4,506.29 ms | 4,721.42 ms | 4,929.62 ms | **4,929.62 ms** |

> [!IMPORTANT]
> **Percentile Pooling vs Arithmetic Averaging:**  
> Percentiles in the aggregate column were computed by pooling the raw latency observations of all 435 successful requests into a single ordered distribution, rather than taking the arithmetic mean of per-run percentiles. If computing across all 450 requests (including timeouts), the pooled percentiles are **p50: 34.49 ms**, **p95: 3,906.49 ms**, and **p99: 5,017.35 ms**.

---

## 3. Historical Benchmark Run Comparison

For historical transparency, the original benchmark run recorded prior to request-level CSV instrumentation yielded:
- **Total Requests Dispatched:** 450 across 3 runs of 150
- **Successful Transfers:** 449 / 450 (99.78%)
- **Server Errors (HTTP 5xx):** 0
- **Client Timeouts:** 1 (0.22%)
- **Balance Invariant:** PASSED ($0.0000 net delta across 10 accounts)
- **Notification Delivery:** 449 / 449 (100.0%)
- **Per-Run Latency Percentiles:**
  - Run 1: p50 = 19.10 ms, p95 = 2,024.72 ms, p99 = 3,305.32 ms
  - Run 2: p50 = 15.45 ms, p95 = 135.67 ms, p99 = 2,464.85 ms
  - Run 3: p50 = 15.84 ms, p95 = 95.36 ms, p99 = 2,661.22 ms
  - *Note on CV figures:* The previously quoted figures ("median about 16 ms, p99 about 2.8 s") were derived by arithmetically averaging these three runs: $(19.10 + 15.45 + 15.84) / 3 = 16.80\text{ ms}$, $(3,305.32 + 2,464.85 + 2,661.22) / 3 = 2,810.46\text{ ms}$.

---

## 4. Historical Single-Replica Baseline

Prior to hardening the transactions service with multi-replica deployments and tuned readiness probes, benchmark experiments on the single-replica deployment (`replicas: 1`, commit `c539795`) produced:
- **Requests Succeeded:** 7 of 300 (2.3% availability)
- **Connection Drops / Timeouts:** 293 of 300 (97.7% failure)
- **Reason:** In a single-replica architecture, terminating the sole container leaves the Kubernetes Service endpoint table completely empty. Replacement pod scheduling, JVM cold boot, HikariCP pool creation, and readiness probe delays take **12 to 14 seconds**, during which 100% of incoming client connections are refused or timed out.
- **Classification:** Labeled as **Historical Baseline**, as reproducing on the current live cluster would require reverting the hardened PostgreSQL schema (which now contains unique idempotency key constraints) and rebuilding legacy Docker images.

---

## 5. Pod Kill Frequency & Chaos Nature

- **Schedule Manifest:** `chaos/pod-kill-experiment.yaml` configured with `schedule: "@every 30s"`.
- **Observed Kills:** 1 to 2 kills occurred per 45-second run (total 5 kills across 135 seconds of load).
- **Wording Accuracy:** The CV and documentation must state **"periodic pod-kill chaos every 30 seconds"** rather than "continuous pod kills", reflecting the discrete scheduled execution.
