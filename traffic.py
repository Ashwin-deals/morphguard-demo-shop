"""Traffic generator for the MorphGuard demo shop (standard library only).

    python traffic.py normal --users 10 [--requests 12] [--base http://127.0.0.1:8001]
    python traffic.py faulty --users 2  [--requests 12] [--base http://127.0.0.1:8001]

normal  N users with random ids from 1000-8999 (random.sample). Each makes R
        requests (default: random 8-14) drawn with the route weights of
        MorphGuard's synthetic Django log (ROUTES in the backend's
        scripts/generate_django_logs.py, copied below).
faulty  Reads GET /_chaos/status first and stops if chaos is off. Users get
        fresh ids from 9001 + a random offset (MorphGuard keeps one block per
        user, so a reused id would add to an old block), default 12 requests
        each, only on the routes the active mode breaks.

Ids are never handed out twice: every id used is appended to .traffic_used_ids
next to this script (MorphGuard ignores lines for a user it has already closed,
so a reused id would be counted as late). Delete that file together with a
processor restart to start over.

One thread per user, 0.05-0.3 s between a user's requests, X-User header on
every request. HTTP errors (the 500s are the point) are counted, never raised.
Redirects are not followed: the login's 302 is the response, so the app logs
exactly the requests this script sends.
"""

import argparse
import json
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

# (method, path template, weight): ROUTES in scripts/generate_django_logs.py.
ROUTES = [
    ("GET", "/", 18), ("GET", "/api/products/{id}", 22), ("GET", "/api/products", 10),
    ("GET", "/api/orders/{id}", 8), ("POST", "/api/cart", 9), ("GET", "/api/cart", 9),
    ("POST", "/api/checkout", 4), ("GET", "/api/reports/{id}", 3), ("POST", "/accounts/login", 5),
    ("GET", "/static/app.js", 12),
]
# The routes each chaos mode breaks in demo_shop.py.
FAULT_ROUTES = {
    "500_burst": [("POST", "/api/checkout"), ("GET", "/api/orders/{id}"), ("POST", "/api/cart")],
    "slow": [("GET", "/api/reports/{id}"), ("GET", "/api/products/{id}")],
    "db_errors": [("GET", "/api/orders/{id}")],
}
CHAOS_USERS_FROM = 9001
USED_IDS_FILE = Path(__file__).resolve().with_name(".traffic_used_ids")
TIMEOUT_SECONDS = 15  # slow mode takes up to ~6 s per request


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None  # urllib then raises HTTPError(302), counted like any other status


OPENER = urllib.request.build_opener(_NoRedirect)


class Results:
    def __init__(self):
        self.lock = threading.Lock()
        self.status = Counter()
        self.errors = Counter()

    def add_status(self, code):
        with self.lock:
            self.status[code] += 1

    def add_error(self, kind):
        with self.lock:
            self.errors[kind] += 1


def send(base: str, method: str, path: str, user: int, results: Results):
    request = urllib.request.Request(base + path, method=method, headers={"X-User": str(user)},
                                     data=b"" if method == "POST" else None)
    try:
        with OPENER.open(request, timeout=TIMEOUT_SECONDS) as response:
            response.read()
            results.add_status(response.status)
    except urllib.error.HTTPError as exc:  # 3xx (not followed), 4xx, 5xx
        exc.read()
        results.add_status(exc.code)
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
        reason = getattr(exc, "reason", exc)
        results.add_error(type(reason).__name__ if not isinstance(reason, str) else reason)


def fill(template: str) -> str:
    return template.format(id=random.randint(1, 5000))


def run_user(base, user, n_requests, routes, weights, results):
    for i in range(n_requests):
        method, template = random.choices(routes, weights=weights)[0]
        send(base, method, fill(template), user, results)
        if i < n_requests - 1:
            time.sleep(random.uniform(0.05, 0.3))


def chaos_status(base: str) -> str:
    try:
        with OPENER.open(base + "/_chaos/status", timeout=5) as response:
            return json.loads(response.read())["chaos"]
    except (urllib.error.URLError, OSError, ValueError, KeyError) as exc:
        sys.exit(f"Cannot read {base}/_chaos/status ({exc}). Is the demo shop running at {base}?")


def load_used_ids() -> set:
    try:
        return {int(x) for x in USED_IDS_FILE.read_text().split()}
    except FileNotFoundError:
        return set()


def remember_ids(ids):
    with USED_IDS_FILE.open("a") as handle:
        handle.write("".join(f"{i}\n" for i in ids))


def fresh_normal_ids(n: int, used: set) -> list:
    pool = sorted(set(range(1000, 9000)) - used)
    if len(pool) < n:
        sys.exit(f"Only {len(pool)} unused normal ids left. Restart the processor and delete {USED_IDS_FILE.name}.")
    return random.sample(pool, n)


def fresh_faulty_ids(n: int, used: set) -> list:
    for _ in range(1000):
        start = CHAOS_USERS_FROM + random.randrange(0, 10_000_000, 100)
        ids = list(range(start, start + n))
        if not used.intersection(ids):
            return ids
    sys.exit(f"Could not find {n} unused faulty ids. Restart the processor and delete {USED_IDS_FILE.name}.")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("kind", choices=("normal", "faulty"))
    parser.add_argument("--users", type=int, required=True)
    parser.add_argument("--requests", type=int, default=None,
                        help="requests per user (default: normal random 8-14, faulty 12)")
    parser.add_argument("--base", default="http://127.0.0.1:8001")
    args = parser.parse_args()
    base = args.base.rstrip("/")
    if args.users < 1:
        sys.exit("--users must be at least 1")

    if args.kind == "normal":
        users = fresh_normal_ids(args.users, load_used_ids())
        routes = [(m, p) for m, p, _ in ROUTES]
        weights = [w for _, _, w in ROUTES]
        counts = {u: args.requests or random.randint(8, 14) for u in users}
        print(f"normal: {len(users)} users (ids {min(users)}-{max(users)}), "
              f"{sum(counts.values())} requests ({min(counts.values())}-{max(counts.values())} each), "
              f"generator route weights, against {base}", flush=True)
    else:
        mode = chaos_status(base)
        if mode == "off":
            sys.exit(f"Chaos is off, so faulty traffic would look normal. Turn a mode on first, e.g.:\n"
                     f"  curl -X POST {base}/_chaos/500_burst     (or slow, db_errors)")
        if mode not in FAULT_ROUTES:
            sys.exit(f"Unknown chaos mode {mode!r} reported by {base}/_chaos/status")
        users = fresh_faulty_ids(args.users, load_used_ids())
        routes = FAULT_ROUTES[mode]
        weights = [1] * len(routes)
        counts = {u: args.requests or 12 for u in users}
        print(f"faulty ({mode}): {len(users)} users (ids {users[0]}-{users[-1]}), {counts[users[0]]} requests each "
              f"on {', '.join(f'{m} {p}' for m, p in routes)}, against {base}", flush=True)

    remember_ids(users)
    results = Results()
    started = time.monotonic()
    threads = [threading.Thread(target=run_user, args=(base, u, counts[u], routes, weights, results), daemon=True)
               for u in users]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    status = " ".join(f"{code}:{n}" for code, n in sorted(results.status.items()))
    errors = ", ".join(f"{k}:{n}" for k, n in results.errors.items()) or "none"
    print(f"done in {time.monotonic() - started:.1f} s: {sum(results.status.values())} responses ({status}), "
          f"connection errors: {errors}", flush=True)


if __name__ == "__main__":
    main()
