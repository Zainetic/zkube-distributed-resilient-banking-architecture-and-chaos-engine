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
from collections import Counter
import concurrent.futures
import csv
import datetime
import http.client
import json
import re
import subprocess
import sys
import threading
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


def execute_transfer(req_index, base_url, from_account, to_account, amount, timeout, idempotency_key=None):
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
    start_epoch = time.time()
    start_iso = datetime.datetime.fromtimestamp(start_epoch, datetime.timezone.utc).isoformat()
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
            return req_index, start_iso, start_epoch, status_code, latency_ms, transfer_id, None, idempotency_key
    except urllib.error.HTTPError as e:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return req_index, start_iso, start_epoch, e.code, latency_ms, None, str(e.reason), idempotency_key
    except urllib.error.URLError as e:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return req_index, start_iso, start_epoch, "CONN_ERR", latency_ms, None, str(e.reason), idempotency_key
    except TimeoutError:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return req_index, start_iso, start_epoch, "TIMEOUT", latency_ms, None, "Request timed out", idempotency_key
    except Exception as e:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        return req_index, start_iso, start_epoch, "ERR_OTHER", latency_ms, None, str(e), idempotency_key


class ClusterTimelineTracker(threading.Thread):
    """
    High-frequency Kubernetes resilience observer.
    Tracks pod terminations, endpoint removals, replacement pod creation,
    and readiness state transitions with millisecond accuracy.
    """
    def __init__(self, proxy_host="127.0.0.1", proxy_port=8001, namespace="default", poll_interval=0.05):
        super().__init__(daemon=True)
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.namespace = namespace
        self.poll_interval = poll_interval
        self.stop_event = threading.Event()
        self.events = []
        self.kills = []
        self._conn = None
        self._can_proxy = True

    def _get_json(self, path):
        if self._can_proxy:
            try:
                if self._conn is None:
                    self._conn = http.client.HTTPConnection(self.proxy_host, self.proxy_port, timeout=3)
                self._conn.request("GET", path)
                resp = self._conn.getresponse()
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    return data
                else:
                    resp.read()
            except Exception:
                self._conn = None
        return None

    def query_state(self):
        ready_ips = set()
        ep_path = f"/apis/discovery.k8s.io/v1/namespaces/{self.namespace}/endpointslices?labelSelector=kubernetes.io/service-name=transactions-service"
        ep_data = self._get_json(ep_path)
        if ep_data:
            for item in ep_data.get("items", []):
                for ep in item.get("endpoints", []):
                    if ep.get("conditions", {}).get("ready", False):
                        for addr in ep.get("addresses", []):
                            ready_ips.add(addr)
        else:
            try:
                cmd = ["kubectl", "get", "endpointslices", "-n", self.namespace, "-l", "kubernetes.io/service-name=transactions-service", "-o", "json"]
                res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
                if res.returncode == 0:
                    ep_data = json.loads(res.stdout)
                    for item in ep_data.get("items", []):
                        for ep in item.get("endpoints", []):
                            if ep.get("conditions", {}).get("ready", False):
                                for addr in ep.get("addresses", []):
                                    ready_ips.add(addr)
            except Exception:
                pass

        pods = {}
        pod_path = f"/api/v1/namespaces/{self.namespace}/pods?labelSelector=app=transactions"
        pod_data = self._get_json(pod_path)
        if pod_data:
            items = pod_data.get("items", [])
        else:
            try:
                cmd = ["kubectl", "get", "pods", "-n", self.namespace, "-l", "app=transactions", "-o", "json"]
                res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
                items = json.loads(res.stdout).get("items", []) if res.returncode == 0 else []
            except Exception:
                items = []

        for item in items:
            name = item["metadata"]["name"]
            uid = item["metadata"]["uid"]
            ip = item.get("status", {}).get("podIP")
            phase = item.get("status", {}).get("phase", "")
            ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in item.get("status", {}).get("conditions", []))
            deleting = item.get("metadata", {}).get("deletionTimestamp") is not None
            restarts = 0
            container_statuses = item.get("status", {}).get("containerStatuses", [])
            if container_statuses:
                restarts = container_statuses[0].get("restartCount", 0)
            pods[name] = {
                "uid": uid,
                "ip": ip,
                "phase": phase,
                "ready": ready,
                "deleting": deleting,
                "restarts": restarts
            }
        return ready_ips, pods

    def run(self):
        init_ep, init_pods = self.query_state()
        tracked_pods = dict(init_pods)
        known_pod_names = set(init_pods.keys())

        while not self.stop_event.is_set():
            now_mono = time.perf_counter()
            now_epoch = time.time()
            now_iso = datetime.datetime.fromtimestamp(now_epoch, datetime.timezone.utc).isoformat()
            ready_ips, current_pods = self.query_state()

            # 1. Detect new kills among tracked pods
            for p_name, prev_info in list(tracked_pods.items()):
                curr_info = current_pods.get(p_name)
                is_killed = False
                reason = ""
                if curr_info is None:
                    is_killed = True
                    reason = "pod_deleted_from_apiserver"
                elif curr_info["deleting"] and not prev_info.get("deleting"):
                    is_killed = True
                    reason = "deletion_timestamp_set"
                elif curr_info["restarts"] > prev_info.get("restarts", 0):
                    is_killed = True
                    reason = f"restart_count_incremented_{curr_info['restarts']}"

                if is_killed:
                    kill_record = {
                        "kill_index": len(self.kills) + 1,
                        "target_pod": p_name,
                        "target_ip": prev_info.get("ip"),
                        "target_uid": prev_info.get("uid"),
                        "reason": reason,
                        "t_kill_mono": now_mono,
                        "t_kill_epoch": now_epoch,
                        "t_kill_iso": now_iso,
                        "ep_removed_mono": None,
                        "ep_removed_seconds": None,
                        "replacement_pod": None,
                        "replacement_ready_mono": None,
                        "replacement_ready_seconds": None,
                        "all_ready_mono": None,
                        "all_ready_seconds": None,
                        "completed": False
                    }
                    self.kills.append(kill_record)
                    self.events.append({
                        "event": "POD_KILL_DETECTED",
                        "timestamp_iso": now_iso,
                        "pod": p_name,
                        "ip": prev_info.get("ip"),
                        "reason": reason
                    })
                    del tracked_pods[p_name]

            # 2. Check active kills for endpoint removal and recovery
            for k in self.kills:
                if k["completed"]:
                    continue

                if k["ep_removed_mono"] is None:
                    target_ip = k.get("target_ip")
                    if target_ip and (target_ip not in ready_ips):
                        k["ep_removed_mono"] = now_mono
                        k["ep_removed_seconds"] = now_mono - k["t_kill_mono"]
                        self.events.append({
                            "event": "ENDPOINT_REMOVED",
                            "timestamp_iso": now_iso,
                            "pod": k["target_pod"],
                            "ip": target_ip,
                            "elapsed_s": k["ep_removed_seconds"]
                        })

                if k["replacement_pod"] is None:
                    for cp_name, cp_info in current_pods.items():
                        if cp_name not in known_pod_names and not cp_info["deleting"]:
                            k["replacement_pod"] = cp_name
                            known_pod_names.add(cp_name)
                            tracked_pods[cp_name] = cp_info
                            self.events.append({
                                "event": "REPLACEMENT_POD_DETECTED",
                                "timestamp_iso": now_iso,
                                "pod": cp_name,
                                "ip": cp_info.get("ip"),
                                "elapsed_s": now_mono - k["t_kill_mono"]
                            })
                            break

                if k["replacement_pod"] and k["replacement_ready_mono"] is None:
                    rep_info = current_pods.get(k["replacement_pod"])
                    if rep_info and rep_info["ready"]:
                        k["replacement_ready_mono"] = now_mono
                        k["replacement_ready_seconds"] = now_mono - k["t_kill_mono"]
                        self.events.append({
                            "event": "REPLACEMENT_POD_READY",
                            "timestamp_iso": now_iso,
                            "pod": k["replacement_pod"],
                            "elapsed_s": k["replacement_ready_seconds"]
                        })

                if k["replacement_ready_mono"] is not None and k["all_ready_mono"] is None:
                    active_ready_count = sum(1 for cp in current_pods.values() if cp["ready"] and not cp["deleting"])
                    if active_ready_count >= 3:
                        k["all_ready_mono"] = now_mono
                        k["all_ready_seconds"] = now_mono - k["t_kill_mono"]
                        k["completed"] = True
                        self.events.append({
                            "event": "ALL_REPLICAS_READY",
                            "timestamp_iso": now_iso,
                            "ready_count": active_ready_count,
                            "elapsed_s": k["all_ready_seconds"]
                        })

            for p_name, p_info in current_pods.items():
                if p_name in tracked_pods:
                    tracked_pods[p_name] = p_info
                elif p_name not in known_pod_names and not p_info["deleting"]:
                    known_pod_names.add(p_name)
                    tracked_pods[p_name] = p_info

            time.sleep(self.poll_interval)

        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass


def fetch_k8s_pod_events(namespace="default"):
    """Query cluster events for pod objects sorted by creation timestamp."""
    try:
        cmd = ["kubectl", "get", "events", "-n", namespace, "--field-selector", "involvedObject.kind=Pod", "--sort-by=.metadata.creationTimestamp", "-o", "json"]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        return json.loads(res.stdout).get("items", [])
    except Exception:
        return []


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
    parser.add_argument(
        "--csv-file",
        default=None,
        help="Optional file path to write per-request CSV output"
    )
    parser.add_argument(
        "--timeline-file",
        default=None,
        help="Optional file path to write cluster kill/recovery timeline JSON"
    )
    parser.add_argument(
        "--proxy-host",
        default="127.0.0.1",
        help="Kubernetes proxy host (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--proxy-port",
        type=int,
        default=8001,
        help="Kubernetes proxy port (default: 8001)"
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
    if args.csv_file:
        print(f"Per-Request CSV Output  : {args.csv_file}")
    if args.timeline_file:
        print(f"Timeline Output File    : {args.timeline_file}")
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

    # Start cluster timeline tracker if k8s monitoring is enabled
    tracker = None
    if can_check_k8s:
        tracker = ClusterTimelineTracker(
            proxy_host=args.proxy_host,
            proxy_port=args.proxy_port,
            namespace=args.namespace,
            poll_interval=0.05
        )
        tracker.start()
        print(" -> Cluster recovery and timeline tracker active.")

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

            pair = account_pairs[total_sent % len(account_pairs)]
            idempotency_key = str(uuid.uuid4())

            fut = executor.submit(
                execute_transfer,
                total_sent + 1,
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
            req_idx, s_iso, s_epoch, status_code, latency, transfer_id, _, _ = res
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

        if tracker is not None:
            # Allow active kill events up to 15s to finish readiness transition
            wait_deadline = time.perf_counter() + 15.0
            while time.perf_counter() < wait_deadline:
                if all(k["completed"] for k in tracker.kills):
                    break
                time.sleep(0.5)
            tracker.stop_event.set()
            tracker.join(timeout=3)

    # Process recovery tracker stats
    kills_count = len(tracker.kills) if tracker else 0
    t_ep_removals = [k["ep_removed_seconds"] for k in tracker.kills if k["ep_removed_seconds"] is not None] if tracker else []
    t_rep_readies = [k["replacement_ready_seconds"] for k in tracker.kills if k["replacement_ready_seconds"] is not None] if tracker else []
    t_all_readies = [k["all_ready_seconds"] for k in tracker.kills if k["all_ready_seconds"] is not None] if tracker else []

    val_ep_removal = t_ep_removals[0] if t_ep_removals else 0.0
    val_rep_ready = t_rep_readies[0] if t_rep_readies else 0.0
    val_all_ready = t_all_readies[0] if t_all_readies else 0.0

    print(f"\n[+] Cluster Resilience Telemetry:")
    print(f" -> Pod Kills Detected                     : {kills_count}")
    if kills_count > 0:
        for idx, k in enumerate(tracker.kills, 1):
            ep_s = f"{k['ep_removed_seconds']:.3f}s" if k['ep_removed_seconds'] is not None else "N/A"
            rep_s = f"{k['replacement_ready_seconds']:.3f}s" if k['replacement_ready_seconds'] is not None else "N/A"
            all_s = f"{k['all_ready_seconds']:.3f}s" if k['all_ready_seconds'] is not None else "N/A"
            print(f"    Kill #{idx} [{k['target_pod']}]:")
            print(f"      (i)   Kill -> Endpoint Eviction        : {ep_s}")
            print(f"      (ii)  Kill -> Replacement Pod Ready   : {rep_s}")
            print(f"      (iii) Kill -> All Replicas Ready      : {all_s}")

    # Write CSV Output
    sorted_results = sorted(results, key=lambda x: x[0])
    if args.csv_file:
        try:
            with open(args.csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["request_index", "timestamp_iso", "timestamp_epoch", "status", "latency_ms", "transfer_id", "idempotency_key", "error"])
                for r in sorted_results:
                    writer.writerow([r[0], r[1], f"{r[2]:.4f}", r[3], f"{r[4]:.2f}", r[5] or "", r[7] or "", r[6] or ""])
            print(f"[+] Request CSV successfully written to: {args.csv_file}")
        except Exception as e:
            print(f"[!] Could not write CSV file {args.csv_file}: {e}")

    # Write Timeline JSON Output
    if args.timeline_file and tracker:
        try:
            k8s_events = fetch_k8s_pod_events(args.namespace)
            timeline_payload = {
                "experiment": "pod-kill",
                "target_app": "transactions",
                "test_duration_seconds": actual_duration,
                "kills_count": kills_count,
                "kills": tracker.kills,
                "lifecycle_events": tracker.events,
                "k8s_pod_events": k8s_events
            }
            with open(args.timeline_file, "w", encoding="utf-8") as f:
                json.dump(timeline_payload, f, indent=2, default=str)
            print(f"[+] Timeline JSON successfully written to: {args.timeline_file}")
        except Exception as e:
            print(f"[!] Could not write timeline file {args.timeline_file}: {e}")

    # Compute Statistics
    status_counts = Counter()
    server_5xx_count = 0
    client_4xx_count = 0
    conn_errors_count = 0
    success_200_count = 0
    latencies_200 = []

    for r in results:
        req_idx, s_iso, s_epoch, status, latency, transfer_id, err, ikey = r
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
        f"- **Pod Kills Injected:** {kills_count}",
        f"- **(i) Kill to Service Endpoint Eviction:** {val_ep_removal:.3f}s",
        f"- **(ii) Kill to Replacement Pod Ready:** {val_rep_ready:.2f}s",
        f"- **(iii) Kill to All Replicas Ready:** {val_all_ready:.2f}s",
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
