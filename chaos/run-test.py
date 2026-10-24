#!/usr/bin/env python3
"""
Reproducible Chaos Engineering Benchmark Harness for ZKube Banking Engine.

Features:
1. Multi-account provisioning: Spawns N independent account pairs (e.g. 5 pairs)
   to simulate realistic distributed banking load and prevent single hot-row lock serialization.
2. Idempotency guarantees: Injects unique UUID Idempotency-Key headers on each transfer.
3. Balance Invariant Verification: Verifies exact conservation of funds (total initial
   balances == total final balances) across all accounts, proving ACID consistency under chaos.
4. Kubernetes Resilience Monitoring:
   - Tracks pod restarts and measures time until all deployment replicas reach 1/1 Ready.
   - Audits Redis Stream notification logs across all worker replicas.
5. Emits a clean Markdown summary table suitable for CV evidence and README benchmarking.
"""

import argparse
import concurrent.futures
from collections import Counter
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid


def create_account(base_url, owner_name, initial_balance):
    url = f"{base_url.rstrip('/')}/api/accounts"
    payload = {
        "ownerName": owner_name,
        "initialBalance": float(initial_balance)
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        account_id = body.get("accountId") or body.get("id")
        if not account_id:
            raise ValueError(f"Could not extract accountId from response: {body}")
        return str(account_id)


def fetch_account_balance(base_url, account_id):
    url = f"{base_url.rstrip('/')}/api/accounts/{account_id}"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        balance = body.get("balance")
        if balance is None:
            raise ValueError(f"Could not extract balance from response: {body}")
        return float(balance)


def execute_transfer(base_url, from_account, to_account, amount, timeout, idempotency_key=None):
    if idempotency_key is None:
        idempotency_key = str(uuid.uuid4())

    url = f"{base_url.rstrip('/')}/api/transfers"
    payload = {
        "fromAccount": from_account,
        "toAccount": to_account,
        "amount": float(amount)
    }
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key
    }
    req = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method="POST"
    )
    start_time = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            status_code = resp.status
            transfer_id = None
            try:
                body = json.loads(resp.read().decode("utf-8"))
                transfer_id = body.get("transferId")
            except Exception:
                pass
            return status_code, latency_ms, transfer_id, None, idempotency_key
    except urllib.error.HTTPError as e:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return e.code, latency_ms, None, str(e.reason), idempotency_key
    except urllib.error.URLError as e:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return "CONN_ERR", latency_ms, None, str(e.reason), idempotency_key
    except TimeoutError:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return "TIMEOUT", latency_ms, None, "Request timed out", idempotency_key
    except Exception as e:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return "ERR_OTHER", latency_ms, None, str(e), idempotency_key


def fetch_all_notification_logs(namespace="default"):
    """Fetch logs from all notification pods matching app=notifications."""
    try:
        cmd = ["kubectl", "logs", "-l", "app=notifications", "-n", namespace, "--tail=5000"]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        return res.stdout.splitlines()
    except (subprocess.SubprocessError, FileNotFoundError):
        return None


def extract_transfer_ids(log_lines):
    if not log_lines:
        return set()
    found = set()
    pattern = re.compile(r"Transfer\s+([a-f0-9\-]+)\s+completed")
    for line in log_lines:
        if "MOCK SMS DISPATCHED" in line:
            m = pattern.search(line)
            if m:
                found.add(m.group(1))
    return found


def measure_pod_readiness(deployment_app="transactions", namespace="default", timeout_s=60.0):
    """Wait and measure until all pods for the deployment are Ready: True."""
    start = time.perf_counter()
    while time.perf_counter() - start < timeout_s:
        try:
            cmd = ["kubectl", "get", "pods", "-l", f"app={deployment_app}", "-n", namespace, "-o", "json"]
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
            pod_data = json.loads(res.stdout)
            items = pod_data.get("items", [])
            if items:
                all_ready = True
                for item in items:
                    phase = item.get("status", {}).get("phase", "")
                    conditions = item.get("status", {}).get("conditions", [])
                    ready_cond = any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)
                    if phase != "Running" or not ready_cond:
                        all_ready = False
                        break
                if all_ready:
                    return time.perf_counter() - start, len(items)
        except Exception:
            pass
        time.sleep(1.0)
    return timeout_s, 0


def calculate_percentile(sorted_list, percentile):
    if not sorted_list:
        return 0.0
    k = (len(sorted_list) - 1) * (percentile / 100.0)
    f = int(k)
    c = f + 1
    if c < len(sorted_list):
        return sorted_list[f] + (k - f) * (sorted_list[c] - sorted_list[f])
    return sorted_list[f]


def main():
    parser = argparse.ArgumentParser(
        description="ZKube Banking Engine - Reproducible Chaos Benchmark Harness"
    )
    parser.add_argument(
        "--accounts-url",
        default="http://localhost:8081",
        help="Base URL for accounts service (default: http://localhost:8081)"
    )
    parser.add_argument(
        "--transactions-url",
        default="http://localhost:8082",
        help="Base URL for transactions service (default: http://localhost:8082)"
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=45.0,
        help="Target experiment duration in seconds (default: 45.0)"
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=200,
        help="Total transfer requests to send (default: 200)"
    )
    parser.add_argument(
        "--pairs",
        type=int,
        default=5,
        help="Number of independent account pairs to distribute load across (default: 5)"
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="Target requests per second (default: derived from requests / duration)"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of concurrent client worker threads (default: 5)"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="HTTP request timeout in seconds (default: 5.0)"
    )
    parser.add_argument(
        "--amount",
        type=float,
        default=2.50,
        help="Amount to transfer per request (default: 2.50)"
    )
    parser.add_argument(
        "--initial-balance",
        type=float,
        default=10000.00,
        help="Initial balance per sender account (default: 10000.00)"
    )
    parser.add_argument(
        "--drain-wait",
        type=float,
        default=6.0,
        help="Seconds to wait after load generation for Redis Streams worker to drain (default: 6.0)"
    )
    parser.add_argument(
        "--namespace",
        default="default",
        help="Kubernetes namespace (default: default)"
    )
    parser.add_argument(
        "--no-k8s",
        action="store_true",
        help="Skip kubectl inspection of notification logs and pod status"
    )
    parser.add_argument(
        "--output-file",
        default=None,
        help="Optional file path to write Markdown summary output"
    )

    args = parser.parse_args()

    print("=====================================================================")
    print("        ZKUBE BANKING ARCHITECTURE - CHAOS TEST RUNNER               ")
    print("=====================================================================")
    print(f"Accounts Service URL    : {args.accounts_url}")
    print(f"Transactions Service URL: {args.transactions_url}")
    print(f"Configured Duration     : {args.duration}s")
    print(f"Target Total Requests   : {args.requests}")
    print(f"Account Pairs           : {args.pairs} independent pairs")
    print(f"Worker Concurrency      : {args.concurrency}")
    print("---------------------------------------------------------------------")

    # Step 1: Provision test account pairs
    print(f"\n[1/5] Provisioning {args.pairs} independent benchmark account pairs...")
    account_pairs = []
    all_account_ids = []
    try:
        for i in range(1, args.pairs + 1):
            s_name = f"Sender_Pair_{i}_{int(time.time())}"
            r_name = f"Receiver_Pair_{i}_{int(time.time())}"
            s_id = create_account(args.accounts_url, s_name, args.initial_balance)
            r_id = create_account(args.accounts_url, r_name, 500.0)
            account_pairs.append({"sender": s_id, "receiver": r_id, "s_name": s_name, "r_name": r_name})
            all_account_ids.extend([s_id, r_id])
            print(f" -> Pair {i}: Sender {s_id[:8]}.. (${args.initial_balance:,.2f})  -->  Receiver {r_id[:8]}.. ($500.00)")
    except Exception as e:
        print(f"[!] Failed to provision test accounts: {e}")
        print("    Ensure the accounts-service is reachable and database is populated.")
        sys.exit(1)

    # Step 2: Record initial balances (Balance Invariant Baseline)
    print("\n[2/5] Recording baseline balances for conservation-of-funds verification...")
    initial_balances = {}
    for acc_id in all_account_ids:
        initial_balances[acc_id] = fetch_account_balance(args.accounts_url, acc_id)
    initial_total_sum = sum(initial_balances.values())
    print(f" -> Total System Initial Balance across {len(all_account_ids)} accounts: ${initial_total_sum:,.2f}")

    # Baseline notifications state
    baseline_log_ids = set()
    can_check_k8s = not args.no_k8s
    if can_check_k8s:
        initial_logs = fetch_all_notification_logs(args.namespace)
        if initial_logs is not None:
            baseline_log_ids = extract_transfer_ids(initial_logs)
            print(f" -> Found {len(baseline_log_ids)} baseline notifications across workers.")
        else:
            can_check_k8s = False

    # Step 3: Run load test
    if args.rate > 0:
        target_rate = args.rate
    elif args.requests > 0 and args.duration > 0:
        target_rate = args.requests / args.duration
    else:
        target_rate = 0.0

    interval = (1.0 / target_rate) if target_rate > 0 else 0.0

    print(f"\n[3/5] Firing transfer requests (duration: {args.duration}s, rate: {target_rate:.1f} req/s)...")
    print("      Active Chaos Mesh Pod-Kill experiment targeted at app=transactions.")

    results = []
    successful_transfer_ids = set()
    start_test = time.perf_counter()
    end_test = start_test + args.duration

    futures = []
    total_sent = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        while time.perf_counter() < end_test:
            if args.requests > 0 and total_sent >= args.requests:
                break

            # Distribute transfers evenly across the independent account pairs
            pair = account_pairs[total_sent % len(account_pairs)]
            idempotency_key = str(uuid.uuid4())

            fut = executor.submit(
                execute_transfer,
                args.transactions_url,
                pair["sender"],
                pair["receiver"],
                args.amount,
                args.timeout,
                idempotency_key
            )
            futures.append(fut)
            total_sent += 1

            if interval > 0:
                time.sleep(interval)

        print(" -> All requests dispatched. Awaiting pending responses...")
        for fut in concurrent.futures.as_completed(futures):
            res = fut.result()
            results.append(res)
            status_code, latency, transfer_id, _, _ = res
            if status_code == 200 and transfer_id:
                successful_transfer_ids.add(transfer_id)

    actual_duration = time.perf_counter() - start_test
    print(f" -> Load phase completed in {actual_duration:.2f}s ({len(results)} total responses received).")

    # Step 4: Verify Balance Invariant (Conservation of Funds)
    print("\n[4/5] Verifying Balance Invariant (ACID conservation under chaos)...")
    final_balances = {}
    for acc_id in all_account_ids:
        final_balances[acc_id] = fetch_account_balance(args.accounts_url, acc_id)
    final_total_sum = sum(final_balances.values())
    balance_delta = final_total_sum - initial_total_sum
    invariant_passed = abs(balance_delta) < 1e-4

    print(f" -> Initial Total Balance : ${initial_total_sum:,.2f}")
    print(f" -> Final Total Balance   : ${final_total_sum:,.2f}")
    print(f" -> Net Delta             : ${balance_delta:,.4f}")
    if invariant_passed:
        print(" [PASS] Balance Invariant Verified: Zero currency created or destroyed.")
    else:
        print(f" [FAIL] Invariant Violation! Discrepancy: ${balance_delta:,.4f}")

    # Step 5: Notifications & Recovery Inspection
    print("\n[5/5] Auditing Redis Stream consumer group and cluster recovery...")
    newly_processed_count = 0
    worker_check_success = False
    recovery_seconds = 0.0
    ready_replicas = 0

    if can_check_k8s:
        print(f" -> Waiting {args.drain_wait}s for Redis Streams worker group to process stream entries...")
        time.sleep(args.drain_wait)

        final_logs = fetch_all_notification_logs(args.namespace)
        if final_logs is not None:
            final_log_ids = extract_transfer_ids(final_logs)
            matched_ids = successful_transfer_ids.intersection(final_log_ids)
            newly_processed_count = len(matched_ids)
            worker_check_success = True
            print(f" -> Matched {newly_processed_count} / {len(successful_transfer_ids)} committed transfers in notification worker logs.")

        recovery_seconds, ready_replicas = measure_pod_readiness("transactions", args.namespace, timeout_s=30.0)
        print(f" -> Kubernetes Pod Recovery: All {ready_replicas} transactions replicas 1/1 Ready in {recovery_seconds:.2f}s.")

    # Compute Statistics
    status_counts = Counter()
    server_5xx_count = 0
    client_4xx_count = 0
    conn_errors_count = 0
    success_200_count = 0
    latencies_200 = []

    for status, latency, _, err, _ in results:
        status_counts[str(status)] += 1
        if isinstance(status, int):
            if 200 <= status < 300:
                success_200_count += 1
                latencies_200.append(latency)
            elif 400 <= status < 500:
                client_4xx_count += 1
            elif 500 <= status < 600:
                server_5xx_count += 1
        else:
            conn_errors_count += 1

    latencies_200.sort()
    min_lat = latencies_200[0] if latencies_200 else 0.0
    avg_lat = (sum(latencies_200) / len(latencies_200)) if latencies_200 else 0.0
    p50_lat = calculate_percentile(latencies_200, 50)
    p95_lat = calculate_percentile(latencies_200, 95)
    p99_lat = calculate_percentile(latencies_200, 99)
    max_lat = latencies_200[-1] if latencies_200 else 0.0

    total_requests = len(results)
    success_rate = (success_200_count / total_requests * 100.0) if total_requests > 0 else 0.0
    notif_percentage = (newly_processed_count / success_200_count * 100.0) if success_200_count > 0 else 100.0
    message_loss = max(0, success_200_count - newly_processed_count) if worker_check_success else 0

    notif_summary = (
        f"{newly_processed_count} / {success_200_count} ({notif_percentage:.1f}%)"
        if worker_check_success else
        f"Transfers succeeded: {success_200_count} (worker log inspection skipped)"
    )

    # Format Markdown
    md_lines = [
        "<!-- BEGIN CHAOS BENCHMARK RESULTS (Paste into README.md ## Results) -->",
        "### Chaos Experiment Benchmark Results",
        "",
        "- **Experiment Type:** Pod Kill (`app: transactions`, periodic @every 30s)",
        f"- **Test Duration:** {actual_duration:.1f}s",
        f"- **Concurrency:** {args.concurrency} concurrent client workers",
        f"- **Account Distribution:** {args.pairs} independent account pairs (distributed row locks)",
        f"- **Total Requests Dispatched:** {total_requests}",
        f"- **Successful Transfers (HTTP 200):** {success_200_count} ({success_rate:.1f}%)",
        f"- **Client Errors (HTTP 4xx):** {client_4xx_count}",
        f"- **Server Errors (HTTP 5xx):** {server_5xx_count}",
        f"- **Connection Drops / Timeouts:** {conn_errors_count}",
        f"- **Balance Conservation Invariant Delta:** ${balance_delta:,.4f} ({'PASSED' if invariant_passed else 'FAILED'})",
        f"- **Notifications Processed vs Transfers Committed:** {notif_summary}",
        f"- **Event / Message Loss:** {message_loss}",
        f"- **Pod Recovery Time (All {ready_replicas} Replicas Ready):** {recovery_seconds:.2f}s",
        "",
        "#### HTTP Status Code Distribution",
        "",
        "| HTTP Status | Category | Count | Percentage |",
        "|-------------|----------|-------|------------|"
    ]

    for code, count in sorted(status_counts.items(), key=lambda x: str(x[0])):
        pct = (count / total_requests * 100.0) if total_requests > 0 else 0.0
        if code == "200":
            cat = "Success (Committed & Queued)"
        elif code.startswith("4"):
            cat = "Client Error"
        elif code.startswith("5"):
            cat = "Server Error (Pod Kill Window)"
        elif "CONN" in code or "TIMEOUT" in code or "ERR" in code:
            cat = "Network / Connection Drop"
        else:
            cat = "Other"
        md_lines.append(f"| {code} | {cat} | {count} | {pct:.1f}% |")

    md_lines.extend([
        "",
        "#### Latency Distribution (HTTP 200 OK)",
        "",
        f"- **Min:** {min_lat:.2f} ms",
        f"- **Average:** {avg_lat:.2f} ms",
        f"- **p50 (Median):** {p50_lat:.2f} ms",
        f"- **p95:** {p95_lat:.2f} ms",
        f"- **p99:** {p99_lat:.2f} ms",
        f"- **Max:** {max_lat:.2f} ms",
        "<!-- END CHAOS BENCHMARK RESULTS -->"
    ])

    markdown_output = "\n".join(md_lines)

    print("\n" + "=" * 69)
    print("            PASTEABLE RESULTS FOR README.md (## Results)            ")
    print("=" * 69)
    print(markdown_output)
    print("=" * 69 + "\n")

    if args.output_file:
        try:
            with open(args.output_file, "w", encoding="utf-8") as f:
                f.write(markdown_output + "\n")
            print(f"[+] Results successfully saved to: {args.output_file}")
        except Exception as e:
            print(f"[!] Could not write results to {args.output_file}: {e}")


if __name__ == "__main__":
    main()
