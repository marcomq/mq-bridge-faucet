"""Writes the benchmark input: N records as data.jsonl and data.csv in target/bench."""
import csv, json, random, sys
from pathlib import Path

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
OUT = Path(__file__).resolve().parent.parent / "target" / "bench"
OUT.mkdir(parents=True, exist_ok=True)

random.seed(1)
with open(OUT / "data.jsonl", "w") as j, open(OUT / "data.csv", "w", newline="") as c:
    w = csv.writer(c)
    w.writerow(["id", "name", "country", "amount", "active"])
    for i in range(N):
        r = {"id": i, "name": f"user{i}", "country": random.choice(["DE", "US", "FR", "JP"]),
             "amount": round(random.random() * 1000, 2), "active": i % 2 == 0}
        j.write(json.dumps(r) + "\n")
        w.writerow([r["id"], r["name"], r["country"], r["amount"], str(r["active"]).lower()])
print(f"{N} records in {OUT}")
