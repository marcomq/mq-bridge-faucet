"""mqb native vs mqb + faucet plugin vs faucet CLI on the same 1M-record data.

Run: uv run --with pyarrow python bench/run.py
Env: MQB, PLUGIN, FAUCET (paths), RUNS (default 3), ONLY=1,3 (case prefixes),
     VARIANT=native,plugin (exact variant names), TIMEOUT (seconds per run, default 300),
     ROUTE='{"concurrency": 4}' (extra mqb route settings).
Input comes from bench/gen_data.py, the CLI from bench/fetch_faucet.sh.
"""
import datetime, json, os, platform, re, shutil, sqlite3, statistics, subprocess, sys, time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WORK = REPO / "target" / "bench"
EXT = "dylib" if platform.system() == "Darwin" else "so"
PLUGIN = os.environ.get("PLUGIN", str(REPO / "target" / "release" / f"libmq_bridge_faucet.{EXT}"))
MQB = os.environ.get("MQB", "mqb")
FAUCET = os.environ.get("FAUCET") or next((str(p) for p in sorted(WORK.glob("faucet-cli-*/*/faucet"))), None)
RUNS = int(os.environ.get("RUNS", "3"))
TIMEOUT = int(os.environ.get("TIMEOUT", "300"))
ONLY = os.environ.get("ONLY", "").split(",") if os.environ.get("ONLY") else None
VARIANTS = os.environ.get("VARIANT", "").split(",") if os.environ.get("VARIANT") else None
ROUTE_EXTRA = json.loads(os.environ.get("ROUTE", "{}"))
OUT = WORK / "out"
JSONL, CSV = str(WORK / "data.jsonl"), str(WORK / "data.csv")
NULL = {"null": None}


def native_in(**extra):
    return {"file": {"path": JSONL, "format": "raw", **extra}}


def plugin(connector, config, **extra):
    return {"custom": {"name": "faucet", "config": {"connector": connector, "config": config, **extra}}}


def cli(type_, config):
    return {"type": type_, "config": config}


def lines(o, name):
    with open(o / name, "rb") as f:
        return sum(1 for _ in f)


def parquet_rows(o, name):
    import pyarrow.parquet as pq
    files = sorted((o / name).rglob("*.parquet"))
    return f"{sum(pq.ParquetFile(f).metadata.num_rows for f in files)} rows / {len(files)} files"


def sqlite_rows(o, name):
    with sqlite3.connect(o / name) as c:
        return c.execute("select count(*) from t").fetchone()[0]


SQLITE_CFG = {"database_url": "{o}/f.db", "table_name": "t", "column_mapping": {"json": {"column": "data"}}}
CLI_SRC = cli("file", {"path": JSONL, "format": "json_lines"})

# (variant, kind, input, output, check); kind "mqb" runs a route, "cli" a faucet pipeline.
CASES = [
    ("1 jsonl read", [
        ("native", "mqb", native_in(), NULL, None),
        ("plugin", "mqb", plugin("file", {"path": JSONL, "format": "json_lines"}), NULL, None),
        ("faucet-cli", "cli", CLI_SRC, None, None),
    ]),
    ("2 csv read", [
        ("native", "mqb", {"file": {"path": CSV, "format": "csv"}}, NULL, None),
        ("plugin", "mqb", plugin("csv", {"path": CSV, "has_headers": True}), NULL, None),
        ("faucet-cli", "cli", cli("csv", {"path": CSV, "has_headers": True}), None, None),
    ]),
    ("3 jsonl write", [
        ("native", "mqb", native_in(), {"file": {"path": "{o}/f.jsonl", "format": "raw"}},
         lambda o: lines(o, "f.jsonl")),
        ("native fsync:batch", "mqb", native_in(), {"file": {"path": "{o}/f.jsonl", "format": "raw", "fsync": "batch"}},
         lambda o: lines(o, "f.jsonl")),
        ("plugin", "mqb", native_in(), plugin("jsonl", {"path": "{o}/f.jsonl", "append": True}),
         lambda o: lines(o, "f.jsonl")),
        ("plugin batch", "mqb", native_in(), plugin("jsonl", {"path": "{o}/f.jsonl", "append": True}, flush="batch"),
         lambda o: lines(o, "f.jsonl")),
        ("faucet-cli", "cli", CLI_SRC, cli("jsonl", {"path": "{o}/f.jsonl", "append": True}),
         lambda o: lines(o, "f.jsonl")),
    ]),
    ("4 parquet write", [
        ("native", "mqb", native_in(), {"object_store": {"url": "local-store://{o}/f/", "format": "parquet"}},
         lambda o: parquet_rows(o, "f")),
        # flush: batch rewrites the file per batch (~250 s); run it with VARIANT="plugin batch".
        ("plugin", "mqb", native_in(), plugin("file", {"path": "{o}/f/", "format": "parquet"}),
         lambda o: parquet_rows(o, "f")),
        ("faucet-cli", "cli", CLI_SRC, cli("file", {"path": "{o}/f/", "format": "parquet"}),
         lambda o: parquet_rows(o, "f")),
    ]),
    ("5 sqlite write", [
        ("native", "mqb", native_in(), {"sqlx": {"url": "sqlite://{o}/f.db?mode=rwc", "table": "t", "auto_create_table": True}},
         lambda o: sqlite_rows(o, "f.db")),
        ("plugin", "mqb", native_in(), plugin("sqlite", SQLITE_CFG), lambda o: sqlite_rows(o, "f.db")),
        ("plugin exactly_once", "mqb", native_in(source_metadata=True),
         plugin("sqlite", SQLITE_CFG, exactly_once=True, state_key="bench"), lambda o: sqlite_rows(o, "f.db")),
        ("faucet-cli", "cli", CLI_SRC, cli("sqlite", SQLITE_CFG), lambda o: sqlite_rows(o, "f.db")),
    ]),
]
OPT_IN = {("4 parquet write", "plugin batch"): ("plugin batch", "mqb", native_in(),
                                           plugin("file", {"path": "{o}/f/", "format": "parquet"}, flush="batch"),
                                           lambda o: parquet_rows(o, "f"))}


def fill(value, o):
    return json.loads(json.dumps(value).replace("{o}", str(o)))


def command(kind, inp, outp, o):
    if kind == "mqb":
        route = {"exit_on_empty": True, **ROUTE_EXTRA, "input": fill(inp, o), "output": fill(outp, o)}
        cfg = {"log_level": "warn", "routes": {"bench": route}}
        path = o / "route.json"
        path.write_text(json.dumps(cfg))
        return [MQB, "--plugin", PLUGIN, "--no-metrics", "--no-ui", "-c", str(path)]
    sink = fill(outp, o) if outp else {"type": "stdout", "config": {}}
    cfg = {"version": 1, "name": "bench", "pipeline": {"sources": {"default": inp}, "sinks": {"default": sink}}}
    path = o / "pipeline.json"
    path.write_text(json.dumps(cfg))
    # Read cases have no sink: --dry-run reads the whole source and writes nothing.
    return [FAUCET, "run", "--log-level", "warn", "--no-env-file", *([] if outp else ["--dry-run"]), str(path)]


def run_once(cmd, o):
    start = time.perf_counter()
    try:
        proc = subprocess.run(["/usr/bin/time", "-l", *cmd], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                              text=True, cwd=o, timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"timeout after {TIMEOUT} s")
    wall = time.perf_counter() - start
    if proc.returncode != 0:
        raise RuntimeError(f"exit {proc.returncode}\n" + "\n".join(proc.stderr.strip().splitlines()[-8:]))
    rss = re.search(r"(\d+)\s+maximum resident set size", proc.stderr)
    return wall, int(rss.group(1)) / 2**20 if rss else float("nan")


def main():
    if not Path(JSONL).exists():
        raise SystemExit("no input data: run bench/gen_data.py first")
    if platform.system() == "Darwin":
        # Idle sleep pauses the runs; perf_counter does not count it, so nothing would show.
        subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())])
    n = sum(1 for _ in open(JSONL, "rb"))
    results = []
    for title, variants in CASES:
        if ONLY and not any(title.startswith(x) for x in ONLY):
            continue
        extra = [v for (t, name), v in OPT_IN.items() if t == title and VARIANTS and name in VARIANTS]
        for variant, kind, inp, outp, check in variants + extra:
            if VARIANTS and variant not in VARIANTS:
                continue
            if kind == "cli" and not FAUCET:
                print(f"skip {title} / {variant}: no faucet CLI, run bench/fetch_faucet.sh", flush=True)
                continue
            walls, rsss, verified, error = [], [], "-", None
            for _ in range(RUNS):
                shutil.rmtree(OUT, ignore_errors=True)
                (OUT / "f").mkdir(parents=True)
                try:
                    wall, rss = run_once(command(kind, inp, outp, OUT), OUT)
                except RuntimeError as e:
                    error = str(e)
                    break
                walls.append(wall)
                print(f"  {title} / {variant}: {wall:.2f} s", file=sys.stderr, flush=True)
                rsss.append(rss)
                if check:
                    verified = str(check(OUT))
            row = dict(case=title, variant=variant, runs=len(walls))
            if walls:
                med = statistics.median(walls)
                row.update(median_s=round(med, 2), min_s=round(min(walls), 2), max_s=round(max(walls), 2),
                           all_s=[round(w, 2) for w in walls], rec_per_s=int(n / med),
                           max_rss_mb=round(max(rsss)), output=verified)
            if error:
                row["error"] = error
            results.append(row)
            print(json.dumps(row), flush=True)
    shutil.rmtree(OUT, ignore_errors=True)
    mqb = subprocess.run([MQB, "--version"], capture_output=True, text=True).stdout.strip()
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    meta = dict(mqb=mqb, plugin=PLUGIN, faucet=FAUCET, records=n, runs=RUNS)
    (WORK / f"results-{stamp}.json").write_text(json.dumps(dict(meta=meta, results=results), indent=1))
    print("\n| case | variant | median s | max s | rec/s | max RSS MB | output |\n|---|---|---|---|---|---|---|")
    for r in results:
        print(f"| {r['case']} | {r['variant']} | {r.get('median_s', 'FAIL')} | {r.get('max_s', '-')} | {r.get('rec_per_s', '-')} "
              f"| {r.get('max_rss_mb', '-')} | {r.get('output', r.get('error', '').splitlines()[0] if r.get('error') else '-')} |")


if __name__ == "__main__":
    main()
