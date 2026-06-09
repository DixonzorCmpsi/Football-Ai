"""Closed-loop load test: hammer Go (:8002) and Python (:8001) identically.

For each (endpoint, concurrency) it fires a fixed number of requests across N
worker threads and reports throughput (req/s) and latency percentiles. Same
client, same machine, same data -> the only variable is the server.
"""
import statistics
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

SERVERS = {"Python": "http://localhost:8001", "Go": "http://localhost:8002"}
ENDPOINTS = ["/current_week", "/tier_list/pool/QB", "/tier_list/pool/WR", "/tier_list/pool/ALL"]
N_REQUESTS = 600
CONCURRENCY = 16


def one(url):
    t0 = time.perf_counter()
    with urllib.request.urlopen(url, timeout=60) as r:
        n = len(r.read())
    return (time.perf_counter() - t0) * 1000.0, n  # ms, bytes


def warm(base, path, n=20):
    for _ in range(n):
        one(base + path)


def run(base, path, n, conc):
    warm(base, path)
    lat = []
    nbytes = 0
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=conc) as ex:
        for ms, b in ex.map(lambda _: one(base + path), range(n)):
            lat.append(ms)
            nbytes = b
    wall = time.perf_counter() - t0
    lat.sort()
    pct = lambda p: lat[min(len(lat) - 1, int(len(lat) * p))]
    return {
        "rps": n / wall,
        "p50": statistics.median(lat),
        "p95": pct(0.95),
        "p99": pct(0.99),
        "mean": statistics.mean(lat),
        "bytes": nbytes,
    }


def main():
    print(f"requests={N_REQUESTS}  concurrency={CONCURRENCY}\n")
    for path in ENDPOINTS:
        res = {}
        for name, base in SERVERS.items():
            res[name] = run(base, path, N_REQUESTS, CONCURRENCY)
        py, go = res["Python"], res["Go"]
        print(f"### {path}   (~{go['bytes']:,} bytes/resp)")
        print(f"{'':10}{'rps':>10}{'mean ms':>10}{'p50 ms':>10}{'p95 ms':>10}{'p99 ms':>10}")
        for name in ("Python", "Go"):
            r = res[name]
            print(f"{name:10}{r['rps']:>10.0f}{r['mean']:>10.1f}{r['p50']:>10.1f}{r['p95']:>10.1f}{r['p99']:>10.1f}")
        print(f"{'speedup':10}{go['rps']/py['rps']:>9.1f}x{py['mean']/go['mean']:>9.1f}x (latency)\n")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        N_REQUESTS = int(sys.argv[1])
    main()
