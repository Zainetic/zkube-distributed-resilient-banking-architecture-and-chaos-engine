#!/usr/bin/env python3
"""
Reproducible Chaos Engineering Benchmark Script for ZKube Banking Engine.

This script:
1. Provisions two bank accounts (sender and receiver) via accounts-service.
2. Captures baseline notification worker state via kubectl logs.
3. Fires N transfer requests at transactions-service across a configured duration
   while a Chaos Mesh Pod-Kill experiment is active.
4. Tracks response codes, 5xx server errors, connection dropouts, and latencies.
5. Verifies asynchronous event processing: checks how many Redis notifications
   were successfully processed by the notifications worker vs transfers committed.
6. Prints a clean Markdown summary formatted for direct insertion into README.md.
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


def create_account(base_url, owner_name, initial_balance):
    url = f"{base_url.rstrip('/')}/api/accounts"
    payload = {
        "ownerName": owner_name,
        "initialBalance": float(initial_balance),
        "balance": float(initial_balance)
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


def execute_transfer(base_url, from_account, to_account, amount, timeout):
    url = f"{base_url.rstrip('/')}/api/transfers"
    payload = {
        "fromAccount": from_account,
        "toAccount": to_account,
        "amount": float(amount)
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
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
            return status_code, latency_ms, transfer_id, None
    except urllib.error.HTTPError as e:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return e.code, latency_ms, None, str(e.reason)
    except urllib.error.URLError as e:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return "CONN_ERR", latency_ms, None, str(e.reason)
    except TimeoutError:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return "TIMEOUT", latency_ms, None, "Request timed out"
    except Exception as e:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return "ERR_OTHER", latency_ms, None, str(e)


def fetch_notification_logs(deployment, namespace):
    try:
        cmd = ["kubectl", "logs", f"deployment/{deployment}", "-n", namespace]
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
        description="ZKube Banking Engine - Reproducible Chaos Benchmark Runner"
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
        default=30.0,
        help="Target experiment duration in seconds (default: 30)"
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=150,
        help="Total transfer requests to send over the duration (default: 150, set 0 for uncapped)"
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
        help="Number of concurrent worker threads (default: 5)"
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
        default=1.00,
        help="Amount to transfer per request (default: 1.00)"
    )
    parser.add_argument(
        "--initial-balance",
        type=float,
        default=1000000.00,
        help="Initial balance for the sender account (default: 1000000.00)"
    )
    parser.add_argument(
        "--drain-wait",
        type=float,
        default=5.0,
        help="Seconds to wait after load generation for notification queue to drain (default: 5.0)"
    )
    parser.add_argument(
        "--notifications-deployment",
        default="notifications-deployment",
        help="Kubernetes deployment name for notifications worker (default: notifications-deployment)"
    )
    parser.add_argument(
        "--namespace",
        default="default",
        help="Kubernetes namespace (default: default)"
    )
    parser.add_argument(
        "--no-k8s",
        action="store_true",
        help="Skip kubectl inspection of notifications worker logs"
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
    print(f"Target Total Requests   : {'Uncapped' if args.requests == 0 else args.requests}")
    print(f"Worker Concurrency      : {args.concurrency}")
    print("---------------------------------------------------------------------")

    # Step 1: Provision test accounts
    print("\n[1/4] Provisioning benchmark bank accounts...")
    try:
        sender_id = create_account(args.accounts_url, "ChaosSender_Alice", args.initial_balance)
        receiver_id = create_account(args.accounts_url, "ChaosReceiver_Bob", 100.0)
        print(f" -> Sender Account Created   : {sender_id} (${args.initial_balance:,.2f})")
        print(f" -> Receiver Account Created : {receiver_id} ($100.00)")
    except Exception as e:
        print(f"[!] Failed to provision test accounts: {e}")
        print("    Ensure the accounts-service is reachable and database is populated.")
        sys.exit(1)

    # Step 2: Capture initial notifications state
    baseline_log_ids = set()
    can_check_k8s = not args.no_k8s
    if can_check_k8s:
        print("\n[2/4] Querying baseline notification worker logs via kubectl...")
        initial_logs = fetch_notification_logs(args.notifications_deployment, args.namespace)
        if initial_logs is not None:
            baseline_log_ids = extract_transfer_ids(initial_logs)
            print(f" -> Found {len(baseline_log_ids)} historical notifications in worker log.")
        else:
            print(" -> [!] kubectl logs command returned non-zero or kubectl not found.")
            print("        Proceeding without automated notification log verification.")
            can_check_k8s = False
    else:
        print("\n[2/4] Skipping kubectl notification log check (--no-k8s set).")

    # Step 3: Run load test
    if args.rate > 0:
        target_rate = args.rate
    elif args.requests > 0 and args.duration > 0:
        target_rate = args.requests / args.duration
    else:
        target_rate = 0.0  # Max throughput uncapped

    interval = (1.0 / target_rate) if target_rate > 0 else 0.0

    print(f"\n[3/4] Firing transfer requests (duration: {args.duration}s, target rate: {target_rate:.1f} req/s)...")
    print("      Ensure your Chaos Mesh Pod Kill experiment is ACTIVE now.")

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

            fut = executor.submit(
                execute_transfer,
                args.transactions_url,
                sender_id,
                receiver_id,
                args.amount,
                args.timeout
            )
            futures.append(fut)
            total_sent += 1

            if interval > 0:
                time.sleep(interval)

        # Wait for all outstanding requests
        print(" -> All requests dispatched. Awaiting pending responses...")
        for fut in concurrent.futures.as_completed(futures):
            res = fut.result()
            results.append(res)
            status_code, latency, transfer_id, _ = res
            if status_code == 200 and transfer_id:
                successful_transfer_ids.add(transfer_id)

    actual_duration = time.perf_counter() - start_test
    print(f" -> Load phase completed in {actual_duration:.2f}s ({len(results)} total responses received).")

    # Step 4: Drain wait and count processed notifications
    newly_processed_count = 0
    worker_check_success = False
    if can_check_k8s:
        print(f"\n[4/4] Waiting {args.drain_wait}s for Redis notification worker to drain queue...")
        time.sleep(args.drain_wait)
        final_logs = fetch_notification_logs(args.notifications_deployment, args.namespace)
        if final_logs is not None:
            final_log_ids = extract_transfer_ids(final_logs)
            matched_ids = successful_transfer_ids.intersection(final_log_ids)
            newly_processed_count = len(matched_ids)
            worker_check_success = True
            print(f" -> Successfully matched {newly_processed_count} / {len(successful_transfer_ids)} committed transfers in worker logs.")
        else:
            print(" -> [!] Failed to retrieve post-test worker logs.")

    # Tally metrics
    status_counts = Counter()
    server_5xx_count = 0
    client_4xx_count = 0
    conn_errors_count = 0
    success_200_count = 0
    latencies_200 = []

    for status, latency, _, err in results:
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
    message_loss = max(0, success_200_count - newly_processed_count) if worker_check_success else "N/A (kubectl skipped)"

    notif_summary = (
        f"{newly_processed_count} / {success_200_count} ({notif_percentage:.1f}%)"
        if worker_check_success else
        f"Transfers succeeded: {success_200_count} (worker log inspection skipped)"
    )

    # Build Markdown Summary
    md_lines = [
        "<!-- BEGIN CHAOS BENCHMARK RESULTS (Paste into README.md ## Results) -->",
        "### Chaos Experiment Benchmark Results",
        "",
        "- **Experiment Type:** Pod Kill (`app: transactions`)",
        f"- **Test Duration:** {actual_duration:.1f}s",
        f"- **Concurrency:** {args.concurrency} workers",
        f"- **Total Requests:** {total_requests}",
        f"- **Successful Transfers (HTTP 200):** {success_200_count} ({success_rate:.1f}%)",
        f"- **Client Errors (HTTP 4xx):** {client_4xx_count}",
        f"- **Server Errors (HTTP 5xx):** {server_5xx_count}",
        f"- **Connection Errors / Timeouts:** {conn_errors_count}",
        f"- **Notifications Processed vs Transfers Succeeded:** {notif_summary}",
        f"- **Event / Message Loss:** {message_loss}",
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
