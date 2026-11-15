"""Check demo_shop.log against MorphGuard's own Django parser.

Imports DjangoParser from the backend repo read-only: run it with
PYTHONDONTWRITEBYTECODE=1 and the backend's venv Python (it needs pandas), so
nothing is written into the repo. Per line it checks: the parser accepts it
(event_type is not "OTHER"), block_id == "user-<id>" from the line's user=,
and the line has user=. Phases come from demo_shop.phases (verify_chaos.sh).

    PYTHONDONTWRITEBYTECODE=1 <backend>/venv/bin/python check_log.py --backend <backend>
"""

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", type=Path,
                    default=Path.home() / "Documents/final-year-project/large-scale-log-analytics-backend")
    args = ap.parse_args()
    if not sys.flags.dont_write_bytecode:
        sys.exit("run with PYTHONDONTWRITEBYTECODE=1 so nothing is written into the backend repo")
    sys.path.insert(0, str(args.backend))
    from parser.django_parser import DjangoParser

    vocab = json.loads((args.backend / "data/features/django/category_encoders.json").read_text())
    vocab = vocab.get("vocab", vocab)
    known_events, known_components = set(vocab["event_type"]), set(vocab["component"])

    lines = (HERE / "demo_shop.log").read_text(encoding="utf-8").splitlines()
    parser = DjangoParser()
    rows = [parser.parse_line(line) for line in lines]
    other = [i for i, r in enumerate(rows) if r["event_type"] == "OTHER"]
    no_user = [i for i, line in enumerate(lines) if " user=" not in line]
    bad_block = [i for i, (line, r) in enumerate(zip(lines, rows))
                 if r["event_type"] != "OTHER" and r["block_id"] != "user-" + re.search(r" user=(\S+)", line).group(1)]
    chaos_lines = [i for i, line in enumerate(lines) if "/_chaos/" in line]

    print(f"demo_shop.log: {len(lines)} lines")
    print(f"  parsed as OTHER (would be dropped): {len(other)}")
    print(f"  block_id != user-<id>: {len(bad_block)}")
    print(f"  lines without user=: {len(no_user)}")
    print(f"  /_chaos/ lines: {len(chaos_lines)}")
    print(f"  blocks: {dict(Counter(r['block_id'] for r in rows))}")
    unseen = Counter(r["event_type"] for r in rows if r["event_type"] not in known_events)
    print(f"  components not in the Django model's vocabulary: {sorted({r['component'] for r in rows} - known_components)}")
    print(f"  event types not in the vocabulary: {dict(unseen) or 'none'}")

    phases = HERE / "demo_shop.phases"
    if phases.exists():
        for entry in phases.read_text().split("\n"):
            if not entry.strip():
                continue
            name, start, end = entry.split()
            chunk = list(zip(lines[int(start):int(end)], rows[int(start):int(end)]))
            print(f"\n-- {name}: {len(chunk)} lines")
            for line, row in chunk[: 5 if name == "baseline" else None]:
                print(f"   {line}")
                print(f"      -> block {row['block_id']}, {row['log_level']} {row['component']}, event \"{row['event_type']}\", size {row['block_size']}")
            if name == "baseline" and len(chunk) > 5:
                print(f"   ... {len(chunk) - 5} more")
    ok = not (other or no_user or bad_block or chaos_lines)
    print("\nRESULT:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
