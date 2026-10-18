# Chaos Testing & Resilience Benchmark Guide

This directory contains the automated benchmark suite for stress-testing the ZKube Banking Engine under active infrastructure faults injected by Chaos Mesh.

The test runner [run-test.py](file:///c:/Users/Neko/Documents/Zain/Java%20Projects/zkube-banking/chaos/run-test.py) automates:
1. **Account Provisioning:** Creates isolated sender and receiver accounts via `accounts-service` (`POST /api/accounts`).
2. **Concurrent Load Generation:** Submits transfer requests to `transactions-service` (`POST /api/transfers`) during active pod failures.
3. **Fault Telemetry:** Tracks HTTP response codes (200, 4xx, 5xx, network drops/timeouts) and response latencies.
4. **Queue Reconciliation:** Verifies eventual consistency by validating that 100% of committed transfers were successfully processed by the asynchronous `notifications-service` worker via Redis `notification_queue`.
5. **Pasteable Output:** Formats the final benchmark summary into GitHub-flavored Markdown ready to be pasted directly into the root [README.md](file:///c:/Users/Neko/Documents/Zain/Java%20Projects/zkube-banking/README.md) under the `## Results` section.

---

## Prerequisites

1. Kubernetes cluster is running with all components deployed:
   ```bash
   kubectl get pods
   kubectl get svc
   ```
2. Chaos Mesh is installed in the `chaos-mesh` namespace and accessible:
   ```bash
   kubectl get pods -n chaos-mesh
   ```
3. Python 3.8+ installed locally (the script uses Python standard library only; no external `pip` dependencies are required).

---

## Running the Benchmark Alongside Chaos Mesh

### Step 1: Start Chaos Mesh Experiment

You can inject faults using either the Chaos Mesh Web Dashboard or a declarative manifest.

#### Option A: Via Chaos Mesh Dashboard
1. Ensure the port-forward tunnel is active:
   ```bash
   kubectl port-forward -n chaos-mesh svc/chaos-dashboard 2333:2333
   ```
2. Open `http://localhost:2333` in your browser and log in with your `chaos-admin` token.
3. Click **New Experiment** -> **Pod Fault** -> **Pod Kill**.
4. Configure target:
   - **Namespace:** `default`
   - **Label Selectors:** `app: transactions`
5. Configure schedule:
   - Select **Continuous** or **Recurring** with a 10–15 second interval.
6. Submit the experiment.

#### Option B: Via Declarative YAML Manifest
Apply a `PodChaos` custom resource:

```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: transactions-pod-kill
  namespace: default
spec:
  action: pod-kill
  mode: one
  selector:
    namespaces:
      - default
    labelSelectors:
      app: transactions
  scheduler:
    cron: "@every 15s"
```

Apply to the cluster:
```bash
kubectl apply -f chaos/pod-kill-experiment.yaml
```

---

### Step 2: Execute the Benchmark Script

Run the script from the root directory while the Chaos Mesh experiment is running:

```bash
# Standard 60-second test with 300 transfer requests across 5 concurrent workers
python chaos/run-test.py --duration 60 --requests 300 --concurrency 5

# 30-second quick validation
python chaos/run-test.py --duration 30 --requests 150

# Save benchmark results directly to a file
python chaos/run-test.py --duration 60 --requests 300 --output-file chaos/results.md
```

#### Available CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--accounts-url` | `http://localhost:8081` | Base URL for the accounts service |
| `--transactions-url` | `http://localhost:8082` | Base URL for the transactions service |
| `--duration` | `30.0` | Total duration of the load phase (in seconds) |
| `--requests` | `150` | Total number of transfer requests to send (`0` for uncapped) |
| `--rate` | `0.0` | Target requests per second (`0` calculates from `requests / duration`) |
| `--concurrency` | `5` | Number of concurrent HTTP worker threads |
| `--timeout` | `5.0` | Timeout per HTTP request (in seconds) |
| `--amount` | `1.00` | Dollar amount per transfer request |
| `--initial-balance` | `1000000.00` | Initial balance for test sender account |
| `--drain-wait` | `5.0` | Seconds to wait post-test for worker to drain Redis queue |
| `--notifications-deployment`| `notifications-deployment` | Deployment name of notifications worker |
| `--namespace` | `default` | Kubernetes namespace |
| `--no-k8s` | `false` | Skips `kubectl logs` inspection of notifications worker |
| `--output-file` | `None` | Path to save the Markdown results table |

---

## Measuring Pod Recovery Time with `kubectl`

While Chaos Mesh kills pods, monitor and measure how rapidly Kubernetes restores service availability:

### Method 1: Watch Pod State Transitions (Interactive)

Open a dedicated terminal window and run:

```bash
kubectl get pods -l app=transactions -w
```

Observe the lifecycle transitions:
1. `Terminating` — Container termination initiated by the Chaos Mesh agent.
2. `ContainerCreating` — Kubernetes ReplicaSet creates a replacement pod.
3. `Running` (0/1 Ready) — JVM startup and Javalin initialization.
4. `Running` (1/1 Ready) — `livenessProbe` and `readinessProbe` at `/api/transactions/health` pass.

### Method 2: Inspect Kubernetes Event Timestamps

Query cluster events sorted by creation timestamp:

```bash
kubectl get events -n default --field-selector involvedObject.kind=Pod --sort-by='.metadata.creationTimestamp'
```

Key events to check:
- `Killing`: Time when pod deletion signal was sent.
- `Scheduled`: Time when replacement pod was scheduled on a node.
- `Started`: Time when the container process booted.
- Calculate: `Time(Started) - Time(Killing) = Pod Recovery Latency`.

### Method 3: Query Precise Timestamps via JSONPath

Extract startup and readiness timestamps directly from the pod status object:

```bash
kubectl get pods -l app=transactions -o jsonpath='{range .items[*]}{"Pod: "}{.metadata.name}{"\n  Created: "}{.metadata.creationTimestamp}{"\n  Started: "}{.status.startTime}{"\n  Ready:   "}{.status.conditions[?(@.type=="Ready")].lastTransitionTime}{"\n\n"}{end}'
```

Calculate recovery duration:
$$\text{Readiness Duration} = \text{lastTransitionTime (Ready)} - \text{creationTimestamp}$$

### Method 4: Monitor Service Endpoint Rerouting

Check when Kubernetes automatically removes unhealthy pods from endpoints and re-adds new ones:

```bash
kubectl get endpoints transactions-service -w
```

---

## Pasting Results into the Main README

When [run-test.py](file:///c:/Users/Neko/Documents/Zain/Java%20Projects/zkube-banking/chaos/run-test.py) finishes, it outputs a Markdown block enclosed between:

```markdown
<!-- BEGIN CHAOS BENCHMARK RESULTS (Paste into README.md ## Results) -->
...
<!-- END CHAOS BENCHMARK RESULTS -->
```

1. Copy the generated block from your terminal output (or from the `--output-file`).
2. Open the root [README.md](file:///c:/Users/Neko/Documents/Zain/Java%20Projects/zkube-banking/README.md).
3. Locate the `## Results` section at the bottom:
   ```markdown
   ## Results

   TODO
   ```
4. Replace `TODO` with the copied Markdown block.
