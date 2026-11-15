# MorphGuard demo shop

A small Django shop that writes one request log line per request, publishes it
straight to Kafka (`logs.raw.django`), and lets MorphGuard's stream processor
score each user live. A chaos switch makes the app really fail for chosen users,
so you can watch MorphGuard catch it.

```
demo_shop.py ──(KafkaLogHandler, key user-<id>)──> Kafka logs.raw.django
   ──> streaming.processor --source django ──> alerts (Kafka + MongoDB)
   ──> backend /api/stream/* ──> dashboard Live Stream page
```

Nothing here changes the MorphGuard repos; they are only read and run.

## Files

| File | What it is |
|---|---|
| `demo_shop.py` | The Django app (single file). Routes: `/`, `/api/products`, `/api/products/<id>`, `/api/orders/<id>`, `GET`/`POST /api/cart`, `POST /api/checkout`, `/api/reports/<id>`, `POST /accounts/login`, `/static/app.js`, plus `/_chaos/*`. |
| `kafka_log_handler.py` | `logging.Handler` that sends each line to Kafka. Never blocks or raises; counts delivered/failed. |
| `traffic.py` | Traffic generator: `normal` and `faulty` users (standard library only). |
| `check_log.py` | Runs every line of `demo_shop.log` through MorphGuard's own `DjangoParser`. |
| `verify_chaos.sh` | curl walk-through of every chaos mode for a chaos user and a normal user. |
| `test_kafka_log_handler.py` | Round trip through Kafka + "broker down never blocks" test. |
| `DEMO_SCRIPT.md` | The 3-minute live demo. |
| `requirements.txt` | Pinned Django and confluent-kafka versions. |

## Install (once)

```bash
cd ~/Documents/final-year-project/morphguard-demo-shop
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt             # pinned: the versions the end-to-end test ran on
```

The MorphGuard backend needs its own venv (`large-scale-log-analytics-backend/venv`,
from its README) and a `.env` with `MONGO_URI` (alerts are stored in MongoDB).

## Start order (5 terminals)

Each command runs in its own new terminal, so every command spells out its full path.

Ports on this laptop: **8000 is taken by the Vagrant VMware Utility** (it accepts
connections and never answers), so the shop runs on **8001**. **5000 is taken by an
old backend** started on 12 Nov with old code; the tested setup runs the current
backend on **5001**. If you stop the old one (`kill 15709`), you can use
`python3 app.py` on 5000 and a plain `npm run dev` instead.

```bash
# 1. Kafka (broker + topics). Run kafka-init once if logs.raw.django is missing.
cd ~/Documents/final-year-project/large-scale-log-analytics-backend && docker compose up -d && docker compose run --rm kafka-init
docker exec lsla-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list   # must list logs.raw.django

# 2. Backend API on :5001
cd ~/Documents/final-year-project/large-scale-log-analytics-backend && source venv/bin/activate && flask --app app run --host :: --port 5001 --with-threads

# 3. Frontend on :3000, pointed at :5001
cd ~/Documents/final-year-project/large-scale-log-analytics-frontend && VITE_API_BASE=http://127.0.0.1:5001 npm run dev

# 4. Stream processor for Django (k = 5, blocks finish after 10 s of topic silence)
cd ~/Documents/final-year-project/large-scale-log-analytics-backend && source venv/bin/activate && python3 -m streaming.processor --source django

# 5. Demo shop on :8001, sending to Kafka
cd ~/Documents/final-year-project/morphguard-demo-shop && source .venv/bin/activate && KAFKA_BOOTSTRAP=localhost:9092 python demo_shop.py runserver 8001 --noreload
```

Then open http://localhost:3000, sign in, pick **Django** in the top-bar source
picker, and open **Live Stream**.

## Normal traffic

```bash
cd ~/Documents/final-year-project/morphguard-demo-shop
python3 traffic.py normal --users 20 --base http://127.0.0.1:8001
```

Users get unused ids from 1000-8999 and the same route mix as MorphGuard's Django
training log. 40 users at once is fine: `demo_shop.py` raises the dev server's
connection queue from 10 to 128 (at 10, about 4% of requests were reset at 20 users).
`--base` defaults to http://127.0.0.1:8001.

## Chaos modes (only users with X-User >= 9000 are affected)

```bash
curl -X POST http://127.0.0.1:8001/_chaos/db_errors    # or 500_burst, slow
curl http://127.0.0.1:8001/_chaos/status                # {"chaos": "...", "affects_users_from": 9000}
curl -X POST http://127.0.0.1:8001/_chaos/off
```

| Mode | What really happens | Logged as |
|---|---|---|
| `500_burst` | checkout, orders, POST cart: 0.3-1.0 s delay, then HTTP 500 (89 bytes) | `ERROR django.request ... Internal Server Error` |
| `slow` | reports, product detail: 4-6 s, 15-40 KB | `WARNING app.performance ... Slow request` |
| `db_errors` | orders: 1-3 s, then the view raises `OperationalError` -> HTTP 500 | `ERROR django.db.backends ... OperationalError: ...` |

## Faulty traffic

```bash
curl -X POST http://127.0.0.1:8001/_chaos/db_errors
python3 traffic.py faulty --users 5 --base http://127.0.0.1:8001     # slow: use --users 10
curl -X POST http://127.0.0.1:8001/_chaos/off
```

`faulty` refuses to run while chaos is off. Every run uses fresh user ids
(9001 + a random offset), because MorphGuard keeps one block per user.

What to expect: the **early_warning** alert appears as soon as a faulty user's
5th request is processed; the **block_anomaly** alert and the storage tier appear
about 10 s after traffic stops (the processor's idle flush).

## Reset between runs

1. `curl -X POST http://127.0.0.1:8001/_chaos/off`
2. Stop the processor (Ctrl-C) and start it again: it becomes a new run, and Live
   Stream shows only the newest run. It continues from its last committed offset:
   lines sent while it was down are scored by the new run, older ones are not
   re-read (tested: 224 lines sent while down -> exactly 224 received).
3. Optional: `: > demo_shop.log` to empty the local log file.

`traffic.py` never reuses a user id: every id it hands out is appended to
`.traffic_used_ids`. MorphGuard ignores lines for a user it has already closed in
the same run (counted as `late_events`), so a reused id would lose lines. Delete
`.traffic_used_ids` only together with a processor restart.

## Checks

```bash
.venv/bin/python test_kafka_log_handler.py          # Kafka round trip + broker-down test
./verify_chaos.sh http://127.0.0.1:8001             # every chaos mode with curl
PYTHONDONTWRITEBYTECODE=1 ~/Documents/final-year-project/large-scale-log-analytics-backend/venv/bin/python check_log.py   # every line parses (no OTHER)
docker exec lsla-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 \
  --topic logs.raw.django --from-beginning --max-messages 5 --property print.key=true
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Lines missing after a Kafka outage | The handler keeps unsent lines for 60 s (`message.timeout.ms`); a 10 s broker restart lost 0 of 20 lines in testing, but an outage longer than 60 s loses lines | The app keeps working either way. Avoid restarting Kafka during a demo. |
| No messages in `logs.raw.django` | Topic missing (`UNKNOWN_TOPIC_OR_PART`); app started without `KAFKA_BOOTSTRAP`; Kafka not running | `docker compose run --rm kafka-init`; restart the app with `KAFKA_BOOTSTRAP=localhost:9092`; `docker ps` should show `lsla-kafka` healthy. Lines still appear in `demo_shop.log`; failed sends are only counted, never raised. |
| Processor exits at once | `Subscribed topic not available: logs.raw.django` | Create the topic (row above), start the processor again. |
| Processor prints `0 lines` | Wrong `--source`, or traffic went to a different port/app | Use `--source django`; send traffic with `--base http://127.0.0.1:8001`; check the console consumer. |
| Processor shows lines but no block_anomaly yet | Blocks finish only after 10 s with no new messages | Wait ~10-15 s after traffic stops. Early warning needs 5 requests from one user. |
| `traffic.py` hangs or times out | Pointed at :8000 (Vagrant holds it) | Pass `--base http://127.0.0.1:8001`. |
| Dashboard shows nothing | Source picker not on Django; frontend talking to the old :5000 backend; signed out; no processor run yet ("no processor has reported" state) | Pick Django; start the frontend with `VITE_API_BASE=http://127.0.0.1:5001`; sign in again; start the processor. |
| Line dropped as OTHER (`lines_dropped_unparsed` > 0) | The line does not match MorphGuard's Django `LINE_PATTERN` (format changed, path with spaces, missing `user=`) | Run `check_log.py`; it lists any line that parses as OTHER. Do not change the log format in `demo_shop.py`. |
| Many `ConnectionResetError` in the traffic summary | The shop was started without the queue fix (old `demo_shop.py`) | Restart it with the current `demo_shop.py`; tested at 40 users with 0 drops. |
