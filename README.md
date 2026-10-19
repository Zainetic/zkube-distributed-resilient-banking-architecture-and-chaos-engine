# ZKube Distributed Resilient Banking Architecture & Chaos Engine

[![ZKube FinTech CI Pipeline](https://github.com/Zainetic/zkube-distributed-resilient-banking-architecture-and-chaos-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/Zainetic/zkube-distributed-resilient-banking-architecture-and-chaos-engine/actions/workflows/ci.yml)

A fully containerized, highly available, and fault-tolerant microservices banking engine built to demonstrate modern distributed systems architecture. Orchestrated via Kubernetes, this system features decoupled ACID-compliant financial transactions, asynchronous Redis-based event queuing, and programmatic resilience testing using Chaos Mesh.

This architecture demonstrates that even during infrastructure failures (such as pod termination during active transactions), financial data remains strictly consistent, message queues retain pending events, and the system achieves eventual consistency without dropped requests.

## Architecture Overview

The cluster is divided into independent, scalable domains:

- **Synchronous Accounts Service (REST):** Manages user creation and balance states, persisting directly to PostgreSQL via HikariCP connection pooling and jOOQ.
- **Decoupled Transactions Service (REST -> Redis):** Executes atomic database locks for transferring funds between accounts. Upon successful commit, it publishes a notification payload to a Redis queue rather than waiting for a synchronous notification response.
- **Asynchronous Notifications Worker:** A background consumer that listens to the Redis `notification_queue`. It processes the queue sequentially, ensuring delivery even if the worker pod is killed and restarted.
- **Automated Infrastructure State:** PostgreSQL is bootstrapped automatically via Kubernetes ConfigMaps, ensuring the database schema and indexes are injected and ready before the Java applications boot.

### Architecture Diagram

```mermaid
graph TD
    Client["Client / External Traffic"]

    subgraph K8s["Kubernetes Cluster"]
        subgraph Services["Kubernetes Services"]
            AccountsSvc["accounts-service<br/>(LoadBalancer :8081)"]
            TxSvc["transactions-service<br/>(LoadBalancer :8082)"]
            PostgresSvc["postgres-db<br/>(ClusterIP :5432)"]
            RedisSvc["redis-cache<br/>(ClusterIP :6379)"]
            NotifSvc["notifications-service<br/>(ClusterIP :8083)"]
        end

        subgraph Pods["Workloads / Pods"]
            AccountsPod["accounts-deployment<br/>(accounts-service)"]
            TxPod["transactions-deployment<br/>(transactions-service)"]
            NotifPod["notifications-deployment<br/>(notifications worker)"]
            PostgresPod[("postgres-deployment<br/>(PostgreSQL 16)")]
            RedisPod[("redis-deployment<br/>(Redis 7.2)")]
        end

        subgraph Queues["In-Memory Queue"]
            NotifQueue["Redis List: notification_queue"]
        end
    end

    Client -->|POST /api/accounts<br/>GET /api/accounts/:id| AccountsSvc
    Client -->|POST /api/transfers| TxSvc

    AccountsSvc --> AccountsPod
    TxSvc --> TxPod
    NotifSvc --> NotifPod
    PostgresSvc --> PostgresPod
    RedisSvc --> RedisPod

    AccountsPod -->|HikariCP / SQL| PostgresSvc
    TxPod -->|ACID Transaction / SELECT FOR UPDATE| PostgresSvc
    TxPod -->|LPUSH| RedisSvc
    RedisPod --- NotifQueue
    NotifPod -->|BRPOP| RedisSvc
```

## Key Technical Features

- **Kubernetes Orchestration & Traffic Gating:** Implements declarative Deployments, LoadBalancers, and ClusterIP routing. Strict `livenessProbe` and `readinessProbe` checks ensure traffic is routed only after JVM startup, and unhealthy containers are automatically terminated and restarted. Container resource bounds (`requests` and `limits`) protect cluster memory and CPU capacity.
- **Event-Driven Self-Healing:** The notification worker is decoupled from the transaction execution path with an automatic reconnection loop. If Redis drops or the worker pod restarts, the transaction service continues committing transfers and queuing messages in Redis (`notification_queue`) until the worker drains the backlog.
- **Automated Data Provisioning & Indexing:** Utilizes Kubernetes ConfigMaps and Volume mounts (`/docker-entrypoint-initdb.d/init.sql`) to automate table creation (`accounts` and `transfers`) and foreign key indexing (`idx_transfers_source`, `idx_transfers_target`, `idx_transfers_status`) upon cluster initialization.
- **Chaos Engineering Ready:** Fully integrated with Chaos Mesh to programmatically inject pod-kill and network faults into the live cluster, verifying zero-downtime resilience and recovery under active transactional load.
- **12-Factor Configuration:** Microservices externalize database and cache connections through environment variables, supporting seamless operation across local JVM, Docker Compose, and Kubernetes environments.

## Technology Stack

- **Language:** Java 21
- **Web Framework:** Javalin 6
- **Data Access & Pooling:** jOOQ, HikariCP
- **In-Memory Datastore / Queuing:** Redis 7.2 (Alpine)
- **Relational Database:** PostgreSQL 16 (Alpine)
- **Build & DevOps:** Maven Wrapper (`mvnw` / `mvnw.cmd`), Docker, Docker Compose, Kubernetes (Docker Desktop / Kubeadm)
- **Chaos Engineering:** Helm, Chaos Mesh
- **Benchmark Suite:** Python 3 standard library benchmark runner ([chaos/run-test.py](file:///c:/Users/Neko/Documents/Zain/Java%20Projects/zkube-banking/chaos/run-test.py))
- **Testing:** JUnit 5, REST-assured, Testcontainers

## Configuration & Environment Variables

All microservices support dynamic runtime configuration through environment variables with sensible defaults for local development:

| Variable | Default Value | Description |
|:---|:---|:---|
| `DB_URL` | `jdbc:postgresql://postgres-db:5432/zkube_chaos` | JDBC connection URL for PostgreSQL |
| `DB_USER` | `postgres` | Database user |
| `DB_PASSWORD` | `admin` | Database password |
| `REDIS_HOST` | `redis-cache` | Hostname or service name for Redis |
| `REDIS_PORT` | `6379` | Port for Redis service |

## CI & Tests

Continuous Integration is managed via GitHub Actions in [.github/workflows/ci.yml](file:///c:/Users/Neko/Documents/Zain/Java%20Projects/zkube-banking/.github/workflows/ci.yml).

### CI Pipeline Workflow

- **Trigger Conditions:** Automatically executed on every `push` and `pull_request` targeting `main` and `master` branches.
- **Runner Environment:** Ubuntu latest (`ubuntu-latest`) configured with Eclipse Temurin JDK 21 and automated Maven dependency caching.
- **Build Steps:**
  1. **Accounts Service:** Builds and runs test suites via `mvn clean package` with Testcontainers Ryuk enabled (`TESTCONTAINERS_RYUK_DISABLED="false"`).
  2. **Transactions Service:** Validates transaction logic and builds JAR artifacts via `mvn clean package`.
  3. **Notifications Service:** Compiles and packages the background worker service via `mvn clean package`.

### Test Suites

- **Accounts API Integration Tests** ([AccountsApiTest.java](file:///c:/Users/Neko/Documents/Zain/Java%20Projects/zkube-banking/accounts-service/src/test/java/com/zkube/accounts/AccountsApiTest.java)): Validates HTTP endpoints (`POST /api/accounts`) using Javalin and REST-assured, ensuring status codes and response bodies conform to specification.
- **Database Integration Tests** ([DatabaseIntegrationTest.java](file:///c:/Users/Neko/Documents/Zain/Java%20Projects/zkube-banking/accounts-service/src/test/java/com/zkube/accounts/DatabaseIntegrationTest.java)): Uses Testcontainers to spin up an ephemeral `postgres:16-alpine` container, executing schema initialization and verifying JDBC table creation and record insertion.
- **Transfer Domain Logic Tests** ([TransferLogicTest.java](file:///c:/Users/Neko/Documents/Zain/Java%20Projects/zkube-banking/transactions-service/src/test/java/com/zkube/transactions/TransferLogicTest.java)): Verifies business invariant enforcement, validating that transfers with insufficient sender balances throw runtime exceptions and transfers with sufficient balances are approved.

## Local Development & Deployment Guide

This guide walks through deploying the system from scratch to a local Docker Desktop or Kubeadm Kubernetes cluster.

### 1. Build Application JARs and Docker Images

First, package all microservice applications into FAT JARs using the included Maven Wrapper:

```bash
# On Linux / macOS:
./mvnw clean package

# On Windows:
.\mvnw.cmd clean package
```

Next, build the container images:

**Build Accounts Service:**

```bash
cd accounts-service
docker build -t zkube-accounts:latest .
```

**Build Transactions Service:**

```bash
cd ../transactions-service
docker build -t zkube-transactions:latest .
```

**Build Notifications Service:**

```bash
cd ../notifications-service
docker build -t zkube-notifications:latest .
```

**Return to root directory:**

```bash
cd ..
```

---

### Alternative: Run with Docker Compose

For rapid local testing outside Kubernetes, the entire stack can be launched via Docker Compose:

```bash
docker compose up -d
```

To view logs or stop the environment:

```bash
docker compose logs -f
docker compose down
```

---

### 2. Deploy Infrastructure & Databases (Kubernetes)

Deploy the persistence layer first. This applies the ConfigMap containing the SQL initialization scripts, spins up Redis, and boots PostgreSQL:

```bash
kubectl apply -f k8s/01-infrastructure.yaml
```

Wait for the database and cache pods to reach the `Running` state:

```bash
kubectl get pods -w
```

### 3. Deploy the Microservices

Once the database and cache pods are running (1/1 Running), deploy the Java microservices:

```bash
kubectl apply -f k8s/02-microservices.yaml
```

### 4. Verify Cluster Health

Check that all pods are running and stable:

```bash
kubectl get pods
```

Check services and assigned ports:

```bash
kubectl get svc
```

> [!NOTE]
> `accounts-service` is exposed on port 8081 and `transactions-service` on port 8082.

## Chaos Engineering (Resilience Testing)

To prove system fault tolerance, Chaos Mesh is used to simulate server and pod failures during active transaction processing.

### 1. Install Helm (Windows)

If Helm is not installed, install it using the Windows Package Manager:

```bash
winget install Helm.Helm
```

*(Restart your terminal after installation).*

### 2. Install Chaos Mesh Operator

Add the Chaos Mesh repository and install the operator into an isolated namespace:

```bash
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm repo update
helm install chaos-mesh chaos-mesh/chaos-mesh -n chaos-mesh --create-namespace
```

Wait for the Chaos Mesh pods to reach the `Running` state:

```bash
kubectl get pods -n chaos-mesh -w
```

### 3. Access the Chaos Mesh Dashboard

Chaos Mesh provides a web dashboard for configuring and observing experiments.

**A. Open Port-Forwarding (Keep this terminal running):**

```bash
kubectl port-forward -n chaos-mesh svc/chaos-dashboard 2333:2333
```

**B. Generate Authentication Token (Open a new terminal):**

Create Admin ServiceAccount:

```bash
kubectl create serviceaccount chaos-admin -n chaos-mesh
```

Grant Cluster-Admin Role:

```bash
kubectl create clusterrolebinding chaos-admin-binding --clusterrole=cluster-admin --serviceaccount=chaos-mesh:chaos-admin
```

Generate Authentication Token (48-hour expiration):

```bash
kubectl create token chaos-admin -n chaos-mesh --duration=48h
```

> [!NOTE]
> The `cluster-admin` role binding for `chaos-admin` is intended strictly for local lab and development clusters. In production environments, adhere to the principle of least privilege by scoping RBAC permissions to specific namespaces and resources.

**C. Execute the Chaos Experiment:**

1. Copy the generated token.
2. Open `http://localhost:2333` in your browser.
3. Paste the token to authenticate.
4. Create an experiment under **Pod Fault** -> **Pod Kill** targeting `app: transactions` with a continuous or recurring schedule.

### 4. Observe Resilience with the Automated Benchmark Suite

To measure system performance under chaos, run the automated benchmark runner [chaos/run-test.py](file:///c:/Users/Neko/Documents/Zain/Java%20Projects/zkube-banking/chaos/run-test.py):

```bash
python chaos/run-test.py --duration 60 --requests 300 --concurrency 5
```

While the chaos experiment is active:
- The script automatically provisions test accounts, dispatches concurrent transfer requests, and measures latency.
- Simultaneously stream logs from the notification worker to watch asynchronous processing:
  ```bash
  kubectl logs deployment/notifications-deployment -f
  ```
- As pods are terminated by Chaos Mesh, Kubernetes automatically redirects traffic to healthy pods and spins up replacements. Transactions complete successfully (`200 OK`), and the Redis queue safely preserves all notifications until a worker pod processes the backlog.
- Detailed instructions for measuring pod recovery times and interpreting results are documented in [chaos/README.md](file:///c:/Users/Neko/Documents/Zain/Java%20Projects/zkube-banking/chaos/README.md).

## API Documentation

### 1. Create Account

- **Endpoint:** `POST http://localhost:8081/api/accounts`
- **Request Body:**

```json
{
  "ownerName": "Alice",
  "balance": 1000.00
}
```

### 2. Execute Transfer (Atomic)

- **Endpoint:** `POST http://localhost:8082/api/transfers`
- **Request Body:**

```json
{
  "fromAccount": "uuid-of-alice",
  "toAccount": "uuid-of-bob",
  "amount": 150.00
}
```

### 3. Kubernetes Health Probes

Used internally by Kubernetes `livenessProbe` and `readinessProbe` to verify pod availability.

- **Endpoint:** `GET http://localhost:8082/api/transactions/health`
- **Response:**

```json
{
  "status": "UP",
  "service": "transactions"
}
```

*(Health checks are also available on port 8081 at `/api/accounts/health` and on port 8083 at `/api/notifications/health`).*

## Results

<!-- Run python chaos/run-test.py while a chaos experiment is active, then paste the generated benchmark Markdown table here. -->
TODO
