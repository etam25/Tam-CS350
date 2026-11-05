"""Parse a log file containing multiple client REPORT sections (one per
server run, e.g. one run per 'a' value) and compute throughput,
utilization, and response-time stats for each run.

Runs are identified purely by the order their "==== REPORT ====" section
appears in the file (1st REPORT -> a = 1, 2nd REPORT -> a = 2, etc.), since
the report text itself doesn't label which 'a' value it belongs to."""

import re
import statistics
import sys

REPORT_MARKER_RE = re.compile(r"={2,}\s*REPORT\s*={2,}")
LINE_RE = re.compile(
    r"Sent:\s*(\d+)\.(\d+)\s+Recv:\s*(\d+)\.(\d+)\s+Exp:\s*(?:\d+)\.(?:\d+)\s+Len:\s*(\d+)\.(\d+)"
)


def split_into_runs(text):
    markers = list(REPORT_MARKER_RE.finditer(text))
    runs = []
    for i, m in enumerate(markers):
        start = m.end()
        end = markers[i + 1].start() if i + 1 < len(markers) else len(text)
        runs.append(text[start:end])
    return runs


def parse_run(segment):
    requests = []
    for m in LINE_RE.finditer(segment):
        sent_s, sent_ns, recv_s, recv_ns, len_s, len_ns = m.groups()
        requests.append({
            "sent": int(sent_s) + int(sent_ns) / 1e9,
            "recv": int(recv_s) + int(recv_ns) / 1e9,
            "len": int(len_s) + int(len_ns) / 1e9,
        })
    return requests


def compute_stats(requests):
    first_sent = requests[0]["sent"]
    last_recv = requests[-1]["recv"]
    time_window = last_recv - first_sent
    count = len(requests)
    throughput = count / time_window if time_window > 0 else float("inf")

    total_busy = sum(r["len"] for r in requests)
    utilization = (total_busy / time_window * 100) if time_window > 0 else float("inf")

    response_times = [r["recv"] - r["sent"] for r in requests]
    avg_resp = statistics.mean(response_times)
    min_resp = min(response_times)
    max_resp = max(response_times)
    stdev_resp = statistics.stdev(response_times) if count > 1 else 0.0

    return {
        "count": count,
        "time_window": time_window,
        "throughput": throughput,
        "total_busy": total_busy,
        "utilization": utilization,
        "avg_resp": avg_resp,
        "min_resp": min_resp,
        "max_resp": max_resp,
        "stdev_resp": stdev_resp,
    }


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <log_file.txt>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        text = f.read()

    runs = split_into_runs(text)
    if not runs:
        print("No '==== REPORT ====' sections found in file.")
        sys.exit(1)

    results = []
    for i, segment in enumerate(runs, start=1):
        requests = parse_run(segment)
        if not requests:
            print(f"a = {i}: no Sent:/Recv:/Len: lines found in this REPORT section, skipping.")
            continue
        results.append((i, compute_stats(requests)))

    if not results:
        print("No usable data found in any REPORT section.")
        sys.exit(1)

    for a, s in results:
        print(f"===== a = {a} =====")
        print(f"Requests processed   : {s['count']}")
        print(f"Active time window   : {s['time_window']:.6f} s")
        print(f"Average throughput   : {s['throughput']:.6f} requests/sec")
        print(f"Total busy time      : {s['total_busy']:.6f} s")
        print(f"Server utilization   : {s['utilization']:.2f}%")
        print(f"Average response time: {s['avg_resp']:.6f} s")
        print(f"Min response time    : {s['min_resp']:.6f} s")
        print(f"Max response time    : {s['max_resp']:.6f} s")
        print(f"Response time stdev  : {s['stdev_resp']:.6f} s")
        print()

    print("===== Summary (a = 1 .. a = %d) =====" % results[-1][0])
    print(f"{'a':>3} {'count':>6} {'throughput':>12} {'util%':>8} {'avg_resp':>10} "
          f"{'min_resp':>10} {'max_resp':>10} {'stdev':>10}")
    for a, s in results:
        print(f"{a:>3} {s['count']:>6} {s['throughput']:>12.4f} {s['utilization']:>8.2f} "
              f"{s['avg_resp']:>10.6f} {s['min_resp']:>10.6f} {s['max_resp']:>10.6f} {s['stdev_resp']:>10.6f}")

    overall_min_a, overall_min_s = min(results, key=lambda r: r[1]["min_resp"])
    overall_max_a, overall_max_s = max(results, key=lambda r: r[1]["max_resp"])
    print()
    print(f"Overall minimum response time across all {len(results)} runs: "
          f"{overall_min_s['min_resp']:.6f} s (a = {overall_min_a})")
    print(f"Overall maximum response time across all {len(results)} runs: "
          f"{overall_max_s['max_resp']:.6f} s (a = {overall_max_a})")


if __name__ == "__main__":
    main()
