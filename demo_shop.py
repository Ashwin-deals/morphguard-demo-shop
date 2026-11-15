"""MorphGuard demo shop: a single-file Django app whose request logs feed MorphGuard.

Every request (except /_chaos/*) writes ONE line in the format MorphGuard's
Django parser reads (parser/django_parser.py LINE_PATTERN), the same format
its synthetic training log uses:

    [2026-11-15 14:05:34,002] ERROR django.request user=88 ip=10.1.9.4 "POST /api/checkout HTTP/1.1" 500 89 812ms Internal Server Error: /api/checkout

The line goes to demo_shop.log, and to Kafka (KafkaLogHandler, topic
logs.raw.django, key "user-<id>") when KAFKA_BOOTSTRAP is set. The user is the
X-User request header (an integer), or "anon".

Chaos switch (for the demo; affects only users whose X-User is an integer >= 9000):

    POST /_chaos/off | 500_burst | slow | db_errors      (anything else: 400)
    GET  /_chaos/status  -> {"chaos": "<mode>", "affects_users_from": 9000}

    500_burst  POST /api/checkout, GET /api/orders/<id>, POST /api/cart return a
               real 500 (89-byte body) after a real 0.3-1.0 s delay
    slow       GET /api/reports/<id>, GET /api/products/<id> really take 4-6 s and
               return 15,000-40,000 bytes (logged as app.performance "Slow request")
    db_errors  GET /api/orders/<id> takes 1-3 s, then the view raises Django's
               OperationalError; Django answers 500 and the line is logged on
               django.db.backends

/_chaos/* requests are never logged.

Run:
    KAFKA_BOOTSTRAP=localhost:9092 python demo_shop.py runserver 8000 --noreload
    python demo_shop.py runserver 8000 --noreload            # file only, no Kafka
"""

import json
import logging
import os
import random
import secrets
import sys
import threading
import time
from pathlib import Path

import django
from django.conf import settings

BASE_DIR = Path(__file__).resolve().parent
LOG_PATH = BASE_DIR / "demo_shop.log"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "logs.raw.django")
SLOW_MS = 4000
LOGGERS = ("django.request", "app.performance", "django.db.backends")
DB_FAILURE_DETAIL = "OperationalError: could not connect to server: Connection refused"
CHAOS_MODES = ("off", "500_burst", "slow", "db_errors")
CHAOS_USERS_FROM = 9000
ERROR_BODY_BYTES = 89  # the 500 body size in MorphGuard's Django training log


class HasUserFilter(logging.Filter):
    """Pass only records that carry a `user` attribute (our per-request lines).
    Django also logs "Not Found" / "Internal Server Error" on django.request,
    without user=, and those must not reach the file or Kafka."""

    def filter(self, record):
        return hasattr(record, "user")


def kafka_handler_factory(bootstrap, topic):
    sys.path.insert(0, str(BASE_DIR))
    from kafka_log_handler import KafkaLogHandler

    return KafkaLogHandler(bootstrap=bootstrap, topic=topic)


handlers = {
    "file": {"class": "logging.FileHandler", "filename": str(LOG_PATH), "encoding": "utf-8",
             "formatter": "line", "filters": ["has_user"]},
}
if KAFKA_BOOTSTRAP:
    handlers["kafka"] = {"()": kafka_handler_factory, "bootstrap": KAFKA_BOOTSTRAP, "topic": KAFKA_TOPIC,
                         "formatter": "line", "filters": ["has_user"]}

settings.configure(
    DEBUG=False,
    # A fresh random key per process unless one is supplied; never printed or stored.
    SECRET_KEY=os.environ.get("DJANGO_SECRET_KEY") or secrets.token_urlsafe(50),
    ALLOWED_HOSTS=["localhost", "127.0.0.1", "[::1]"],
    ROOT_URLCONF=__name__,
    INSTALLED_APPS=[],
    DATABASES={},
    MIDDLEWARE=[f"{__name__}.RequestLogMiddleware"],
    LOGGING={
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"line": {"format": "[%(asctime)s] %(levelname)s %(name)s %(message)s"}},
        "filters": {"has_user": {"()": HasUserFilter}},
        "handlers": handlers,
        "loggers": {name: {"handlers": list(handlers), "level": "INFO", "propagate": False} for name in LOGGERS},
    },
)
django.setup()

from django.db import OperationalError  # noqa: E402
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse  # noqa: E402
from django.urls import path  # noqa: E402
from django.utils.encoding import escape_uri_path  # noqa: E402
from django.views.decorators.http import require_GET, require_http_methods, require_POST  # noqa: E402

# -- request logging ------------------------------------------------------------


def request_user(request) -> str:
    raw = request.headers.get("X-User", "").strip()
    return str(int(raw)) if raw.isdigit() else "anon"


class RequestLogMiddleware:
    """Times each request and writes one MorphGuard-format line for it."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.path.startswith("/_chaos/"):
            return self.get_response(request)
        request.db_failure = False
        started = time.perf_counter()
        response = self.get_response(request)
        ms = int(round((time.perf_counter() - started) * 1000))
        self.log(request, response, ms)
        return response

    def process_exception(self, request, exception):
        # A view's database error: Django still renders the 500; we only note why.
        if isinstance(exception, OperationalError):
            request.db_failure = True
        return None

    @staticmethod
    def log(request, response, ms):
        status = response.status_code
        req_path = escape_uri_path(request.path)  # no spaces, so the line stays one \S+ path
        if getattr(request, "db_failure", False):
            level, name, detail = logging.ERROR, "django.db.backends", DB_FAILURE_DETAIL
        elif status >= 500:
            level, name, detail = logging.ERROR, "django.request", f"Internal Server Error: {req_path}"
        elif ms >= SLOW_MS:
            level, name, detail = logging.WARNING, "app.performance", "Slow request"
        elif status == 404:
            level, name, detail = logging.WARNING, "django.request", f"Not Found: {req_path}"
        elif status >= 400:
            level, name, detail = logging.WARNING, "django.request", f"{response.reason_phrase}: {req_path}"
        else:
            level, name, detail = logging.INFO, "django.request", ""
        size = len(response.content) if not response.streaming else int(response.get("Content-Length") or 0)
        user = request_user(request)
        ip = request.META.get("REMOTE_ADDR", "0.0.0.0")
        protocol = request.META.get("SERVER_PROTOCOL", "HTTP/1.1")
        line = f'user={user} ip={ip} "{request.method} {req_path} {protocol}" {status} {size} {ms}ms'
        logging.getLogger(name).log(level, line + (f" {detail}" if detail else ""), extra={"user": user})


# -- chaos switch ------------------------------------------------------------------

_chaos = {"mode": "off"}
_chaos_lock = threading.Lock()


def chaos_mode() -> str:
    with _chaos_lock:
        return _chaos["mode"]


def chaos_hits(request, mode: str) -> bool:
    """True when `mode` is on and this request's user is a chaos user (X-User >= 9000)."""
    user = request_user(request)
    return chaos_mode() == mode and user.isdigit() and int(user) >= CHAOS_USERS_FROM


def error_body() -> bytes:
    body = json.dumps({"detail": "Server error."})
    return (body[:-1] + " " * (ERROR_BODY_BYTES - len(body)) + "}").encode()  # exactly 89 bytes of valid JSON


def burst_500():
    time.sleep(random.uniform(0.3, 1.0))
    return HttpResponse(error_body(), status=500, content_type="application/json")


def chaos_status(request):
    if request.method != "GET":
        return JsonResponse({"detail": "Method not allowed."}, status=405)
    return JsonResponse({"chaos": chaos_mode(), "affects_users_from": CHAOS_USERS_FROM})


def chaos_set(request, mode):
    if request.method != "POST":
        return JsonResponse({"detail": "Method not allowed."}, status=405)
    if mode not in CHAOS_MODES:
        return JsonResponse({"detail": f"unknown mode {mode!r}", "modes": list(CHAOS_MODES)}, status=400)
    with _chaos_lock:
        _chaos["mode"] = mode
    return JsonResponse({"chaos": mode, "affects_users_from": CHAOS_USERS_FROM})


# -- realistic response bodies ----------------------------------------------------

WORDS = ("steel kettle walnut desk linen throw ceramic mug oak shelf wool rug brass lamp cotton towel "
         "glass vase leather bag bamboo tray copper pan").split()


def work():
    time.sleep(random.uniform(0.010, 0.060))  # 10-60 ms of fake work


def target(size: int) -> int:
    return int(size * random.uniform(0.7, 1.3))


def product(pid: int) -> dict:
    rng = random.Random(pid)
    name = " ".join(rng.sample(WORDS, 2)).title()
    return {"id": pid, "name": name, "price": round(rng.uniform(4, 240), 2), "stock": rng.randint(0, 80),
            "rating": round(rng.uniform(2.5, 5), 1), "sku": f"SKU-{pid:06d}"}


def json_filled(base: dict, key: str, make_item, size: int, exact: bool = False) -> JsonResponse:
    """`base` with base[key] grown item by item until the body reaches `size`
    bytes (times a random 0.7-1.3, unless `exact`)."""
    goal, items, i = (size if exact else target(size)), [], 0
    base[key] = items
    while len(json.dumps(base)) < goal:
        items.append(make_item(i))
        i += 1
    return JsonResponse(base)


def text_filled(head: str, chunk, tail: str, size: int, content_type: str, min_chunks: int = 0) -> HttpResponse:
    goal, parts, i = target(size), [head], 0
    length = len(head) + len(tail)
    while length < goal or i < min_chunks:
        piece = chunk(i)
        if i >= min_chunks and length + len(piece) > goal:
            break
        parts.append(piece)
        length += len(piece)
        i += 1
    parts.append(tail)
    return HttpResponse("".join(parts), content_type=content_type)


# -- views ---------------------------------------------------------------------


HOME_CSS = (
    ":root{--bg:#f5f3ee;--ink:#1c2433;--mut:#6b7280;--line:#e6e1d6;--teal:#0f766e;--amber:#f59e0b}"
    "*{box-sizing:border-box}body{margin:0;font:15px/1.5 system-ui,-apple-system,'Segoe UI',sans-serif;background:var(--bg);color:var(--ink)}"
    "header{position:sticky;top:0;z-index:2;display:flex;align-items:center;gap:18px;padding:14px 28px;background:var(--ink);color:#fff}"
    ".logo{font-weight:800;font-size:20px}.logo b{color:var(--amber)}"
    ".who{margin-left:auto;font-size:13px;opacity:.9}.who input{width:78px;margin-left:6px;padding:4px 7px;border:0;border-radius:6px}"
    "header button{border:0;border-radius:99px;padding:8px 15px;font:600 14px system-ui;cursor:pointer;background:var(--amber);color:var(--ink)}"
    ".hero,main,footer{max-width:1180px;margin:0 auto}.hero{padding:34px 28px 6px}.hero h1{margin:0;font-size:32px}.hero p{margin:4px 0 0;color:var(--mut)}"
    "main{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:18px;padding:22px 28px 36px}"
    ".card{background:#fff;border:1px solid var(--line);border-radius:14px;overflow:hidden;display:flex;flex-direction:column;transition:.15s}"
    ".card:hover{transform:translateY(-3px);box-shadow:0 8px 22px #1c243318}"
    ".pic{height:118px;display:grid;place-items:center;font-size:48px;background:linear-gradient(135deg,#fbd3c2,#f0c38e)}"
    ".card:nth-child(6n+2) .pic{background:linear-gradient(135deg,#f3eeb0,#c6e98f)}.card:nth-child(6n+3) .pic{background:linear-gradient(135deg,#c9f2c4,#86e0a4)}"
    ".card:nth-child(6n+4) .pic{background:linear-gradient(135deg,#b9f0e3,#8fd9e9)}.card:nth-child(6n+5) .pic{background:linear-gradient(135deg,#c4dcf7,#9aa9ef)}"
    ".card:nth-child(6n) .pic{background:linear-gradient(135deg,#ddc8f5,#c79be8)}"
    ".bd{padding:12px 14px 14px;display:flex;flex-direction:column;gap:3px;flex:1}.bd h2{font-size:16px;margin:0}"
    ".pr{font-weight:800;font-size:19px}.mt{font-size:12px;color:var(--mut)}"
    ".rw{display:flex;gap:8px;margin-top:auto;padding-top:9px}.rw a,.rw button{flex:1;padding:7px;border-radius:8px;font:13px system-ui;cursor:pointer;text-align:center;text-decoration:none}"
    ".rw a{border:1px solid var(--line);color:var(--ink)}.rw button{border:0;background:var(--teal);color:#fff}"
    "#t{position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:var(--ink);color:#fff;padding:10px 18px;border-radius:10px;opacity:0;transition:.25s;pointer-events:none}"
    "#t.on{opacity:1}#t.er{background:#b91c1c}footer{padding:0 28px 28px;color:var(--mut);font-size:12px}"
    "header button{white-space:nowrap}@media(max-width:640px){header{flex-wrap:wrap;gap:10px;padding:12px 16px}.who{margin-left:0}"
    ".hero,main,footer{padding-left:16px;padding-right:16px}.hero h1{font-size:26px}main{grid-template-columns:1fr}}"
)
HOME_JS = (
    "const $=i=>document.getElementById(i);let n=0;"
    "try{$('u').value=localStorage.u||(localStorage.u=1000+Math.floor(Math.random()*8000))}catch(e){$('u').value=1000+Math.floor(Math.random()*8000)}"
    "$('u').onchange=()=>{try{localStorage.u=$('u').value}catch(e){}};"
    "function toast(m,e){const t=$('t');t.textContent=m;t.className='on'+(e?' er':'');clearTimeout(t.h);t.h=setTimeout(()=>t.className='',2600)}"
    "async function go(m,p){try{const r=await fetch(p,{method:m,headers:{'X-User':$('u').value}});"
    "return r.ok?r:(toast('Server error '+r.status+' - the shop is failing for this user',1),null)}catch(e){toast('Shop unreachable',1);return null}}"
    "async function add(){if(await go('POST','/api/cart')){$('n').textContent=++n;toast('Added to cart')}}"
    "async function view(i){const r=await go('GET','/api/products/'+i);if(r){const j=await r.json();"
    "toast(j.name+' - '+j.price+' EUR - '+j.reviews.length+' reviews')}return false}"
    "async function buy(){if(!n)return toast('Your cart is empty');const r=await go('POST','/api/checkout');"
    "if(r){const j=await r.json();n=0;$('n').textContent=0;toast('Order '+j.order_id+' confirmed')}}"
)
ICONS = {"kettle": "\U0001fad6", "desk": "\U0001fa91", "throw": "\U0001f9e3", "mug": "☕", "shelf": "\U0001f4da",
         "rug": "\U0001f9f6", "lamp": "\U0001f4a1", "towel": "\U0001f9fa", "vase": "\U0001f3fa", "bag": "\U0001f45c",
         "tray": "\U0001f37d", "pan": "\U0001f373"}


def product_card(i: int) -> str:
    p = product(i)
    words = p["name"].lower().split()
    icon = next((ICONS[w] for w in reversed(words) if w in ICONS), "\U0001f6cd")
    stock = f'{p["stock"]} in stock' if p["stock"] else "Sold out"
    return (f'<article class="card"><div class="pic">{icon}</div><div class="bd"><h2>{p["name"]}</h2>'
            f'<div class="pr">{p["price"]:.2f} EUR</div><div class="mt">&#9733; {p["rating"]} &middot; {stock}</div>'
            f'<div class="rw"><a href="/api/products/{i}" onclick="return view({i})">View</a>'
            f'<button onclick="add()">Add to cart</button></div></div></article>')


@require_GET
def home(request):
    work()
    head = ('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>Demo Shop</title><link rel="icon" href="data:,"><script src="/static/app.js" defer></script>'
            f'<style>{HOME_CSS}</style></head><body><header><div class="logo">Demo<b>Shop</b></div>'
            '<label class="who">Shopping as user #<input id="u" inputmode="numeric"></label>'
            '<button onclick="buy()">Cart &middot; <span id="n">0</span> &middot; Checkout</button></header>'
            '<section class="hero"><h1>Handpicked for your home</h1><p>Lamps, mugs, rugs and more. Every click here is a real '
            'request that MorphGuard watches live.</p></section><main>')
    tail = (f'</main><footer>Requests are logged per user and streamed to Kafka for MorphGuard. '
            f'Users 9000 and above are affected by the chaos switch.</footer><div id="t"></div></body></html>')
    return text_filled(head, product_card, tail, 5200, "text/html", min_chunks=4)


@require_GET
def products(request):
    work()
    return json_filled({"page": 1}, "results", lambda i: product(i + 1), 9400)


@require_GET
def product_detail(request, pid):
    work()
    rng = random.Random(pid)
    slow = chaos_hits(request, "slow")
    if slow:
        time.sleep(random.uniform(4.0, 6.0))
    return json_filled({**product(pid), "description": " ".join(rng.choices(WORDS, k=20))}, "reviews",
                       lambda i: {"user": rng.randint(1, 5000), "stars": rng.randint(1, 5),
                                  "text": " ".join(rng.choices(WORDS, k=8))},
                       random.randint(15000, 39500) if slow else 1800, exact=slow)


@require_GET
def order_detail(request, oid):
    if chaos_hits(request, "500_burst"):
        return burst_500()
    if chaos_hits(request, "db_errors"):
        time.sleep(random.uniform(1.0, 3.0))  # waiting on a database that never answers
        raise OperationalError("could not connect to server: Connection refused")
    work()
    rng = random.Random(oid)
    return json_filled({"id": oid, "status": rng.choice(["paid", "shipped", "delivered"]), "currency": "EUR"},
                       "lines", lambda i: {**product(rng.randint(1, 5000)), "qty": rng.randint(1, 4)}, 1500)


@require_http_methods(["GET", "POST"])
def cart(request):
    if request.method == "POST" and chaos_hits(request, "500_burst"):
        return burst_500()
    work()
    if request.method == "POST":
        return json_filled({"ok": True, "added": product(random.randint(1, 5000))}, "messages",
                           lambda i: "Item added to your cart", 420)
    return json_filled({"currency": "EUR"}, "items", lambda i: {**product(random.randint(1, 5000)), "qty": 1}, 1100)


@require_POST
def checkout(request):
    if chaos_hits(request, "500_burst"):
        return burst_500()
    work()
    return json_filled({"order_id": random.randint(10000, 99999), "status": "confirmed"}, "notes",
                       lambda i: "Payment captured", 380)


@require_GET
def report(request, rid):
    work()
    rng = random.Random(rid)
    slow = chaos_hits(request, "slow")
    if slow:
        time.sleep(random.uniform(4.0, 6.0))
    return json_filled({"report": rid, "period": "2026-11"}, "rows",
                       lambda i: {"day": i % 30 + 1, "orders": rng.randint(0, 400), "revenue": round(rng.uniform(0, 9000), 2)},
                       random.randint(15000, 39500) if slow else 20400, exact=slow)


@require_POST
def login(request):
    work()
    response = HttpResponseRedirect("/")
    response.content = text_filled("<html><body><p>Signed in. Redirecting to ", lambda i: "/", "</p></body></html>",
                                   310, "text/html").content
    return response


@require_GET
def app_js(request):
    work()
    return text_filled("/* demo-shop bundle */\n" + HOME_JS + "\n",
                       lambda i: f"function m{i}(a,b){{return a&&b?a.concat(b):(a||b||[]).slice({i % 7})}}\n",
                       "window.demoShopBundle = true;\n", 88000, "application/javascript")


def not_found(request, exception=None):
    return JsonResponse({"detail": "Not found.", "path": escape_uri_path(request.path)}, status=404)


def server_error(request):
    return HttpResponse(error_body(), status=500, content_type="application/json")


handler404 = not_found
handler500 = server_error

urlpatterns = [
    path("", home),
    path("api/products", products),
    path("api/products/<int:pid>", product_detail),
    path("api/orders/<int:oid>", order_detail),
    path("api/cart", cart),
    path("api/checkout", checkout),
    path("api/reports/<int:rid>", report),
    path("accounts/login", login),
    path("static/app.js", app_js),
    path("_chaos/status", chaos_status),
    path("_chaos/<str:mode>", chaos_set),
]

if __name__ == "__main__":
    from django.core.management import execute_from_command_line
    from django.core.servers.basehttp import WSGIServer

    # runserver queues only 10 pending connections; 20+ traffic.py users connecting
    # at once overflowed it and got ConnectionResetError (about 4% of requests at 20
    # users). 128 is the macOS default somaxconn.
    WSGIServer.request_queue_size = 128
    execute_from_command_line(sys.argv)
