# MorphGuard live demo (3 minutes)

**Before you start:** all 5 services running (README "Start order"), chaos `off`, a fresh
processor run, dashboard open on **Live Stream** with source **Django**, and a terminal in
`~/Documents/final-year-project/morphguard-demo-shop`. Use `db_errors` for the live fault: in our test it raised an early
warning for every faulty user, and it is fast (1-3 s per request).

> **Reminder: do not click "Run Retrain Cycle" (Models) or "Run Detection Scan" (Dashboard) on HDFS during the demo** - it starts a real ~3-minute GA job and can change the live model.

| Time | Do | Say |
|---|---|---|
| 0:00 | Point at the diagram below. | "This is a small online shop built with Django. Every request writes one log line. The line goes straight to Kafka. MorphGuard's stream processor reads it, groups the lines by user, and scores each user with our models. Alerts appear here on the Live Stream page." |
| 0:30 | `python3 traffic.py normal --users 20 --base http://127.0.0.1:8001` | "First, normal shoppers. Twenty users browse products, use the cart and check out. You can see the line counter go up. There are no early warnings: the traffic looks normal." |
| 0:55 | Wait for the counter to stop. | "About ten seconds after the traffic stops, MorphGuard closes each user and decides where to store their logs. Most normal users go to cold storage, so we keep very little. A few may get a block alert. Those are false alarms, and I will give you the numbers at the end." |
| 1:15 | `curl -X POST http://127.0.0.1:8001/_chaos/db_errors` then `python3 traffic.py faulty --users 5 --base http://127.0.0.1:8001` | "Now I break the app on purpose. The database stops answering for five users. Their requests really fail: they wait one to three seconds and get an error." |
| 1:35 | Point at the alerts feed (it appears about 10 s after you start). | "Here is the early warning. MorphGuard raised it after only five requests from this user, while the user was still active. It says 'block still running'. That is the point: we warn before the problem is over." |
| 2:00 | Wait until traffic stops, then point at the block alerts and the HOT tier. | "When the faulty users finish, the full-block model flags them too, and their logs go to hot storage, so we keep every line of the users that had problems." |
| 2:20 | `curl -X POST http://127.0.0.1:8001/_chaos/off` | "Chaos is off. The shop is healthy again." |
| 2:30 | Say the numbers. | "We tested each failure three times with five users. Every faulty user got a block alert, 45 out of 45. The early warning caught every database-error and every slow user, but only about half of the HTTP 500 burst users, 8 out of 15. Out of 120 normal users, none got an early warning, but on average 7 out of 40, about 17%, got a false block alert." |
| 2:45 | Say the limitation. | "To be honest about the limits: the Django model was trained on synthetic logs, and its held-out F1 is only 0.436. Our faulty users here only send broken requests, which is easier than real life. So this shows that the pipeline works end to end, not that the model is production-ready." |

```
Django shop  ->  Kafka (logs.raw.django)  ->  stream processor (k = 5)  ->  alerts + storage tiers  ->  Live Stream page
```

**If the live app fails:** "If the live app does not work today, we show the 50,000-line Kafka
replay instead." Switch the source picker to **HDFS**, then run
`cd ~/Documents/final-year-project/large-scale-log-analytics-backend && source venv/bin/activate && python3 -m streaming.processor --from-beginning --idle-flush 8 --stop-after-idle 20`
(the `logs.raw` topic already holds the 50,000 replayed lines; if it is empty, also run
`python3 -m streaming.producer --limit 50000 --rate 5000`).
