#!/usr/bin/env python3
"""Round-trips records through faucet connectors with the installed `mqb` CLI.

Each case in cases.toml names a faucet `sink` and a faucet `source`. The runner
writes N records through the sink (a native file feeding a faucet output),
reads them back through the source (a faucet input feeding a native file) and
compares. A case without a sink reads from the runner's HTTP server instead.

    python3 tests/endpoints/run.py                 # everything runnable here
    python3 tests/endpoints/run.py sqlite csv      # cases whose name matches
    python3 tests/endpoints/run.py --list

Needs `mqb` on PATH and the plugin where mqb discovers it, or `--plugin`.
Compiles nothing. Standard library only (Python 3.11+). Cases whose broker
cannot be started are skipped; `--strict` turns a skip into a failure.
"""

import argparse
import concurrent.futures
import http.server
import json
import os
import pathlib
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import uuid
from xml.sax.saxutils import escape

HERE = pathlib.Path(__file__).resolve().parent
RUN = uuid.uuid4().hex[:8]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def substitute(value, values: dict):
    """Replaces `$NAME` in every string of a TOML value, longest name first.
    A string that is only `$NAME` of a number becomes that number (`port = "$PORT"`)."""
    if isinstance(value, str) and isinstance(values.get(value[1:]), int) and value[:1] == "$":
        return values[value[1:]]
    if isinstance(value, str):
        for name in sorted(values, key=len, reverse=True):
            value = value.replace(f"${name}", str(values[name]))
        return value
    if isinstance(value, list):
        return [substitute(v, values) for v in value]
    if isinstance(value, dict):
        return {k: substitute(v, values) for k, v in value.items()}
    return value


def endpoint(spec: dict) -> dict:
    """A table with `connector` is a faucet endpoint; anything else is native.
    `middlewares` stays on the endpoint, outside the faucet config."""
    if "connector" in spec:
        config = {k: v for k, v in spec.items() if k != "middlewares"}
        wrapped = {"custom": {"name": "faucet", "config": config}}
        if "middlewares" in spec:
            wrapped["middlewares"] = spec["middlewares"]
        return wrapped
    return spec


def tail(path: pathlib.Path, lines: int = 12) -> str:
    if not path.exists():
        return ""
    noise = re.compile(r"INFO|DEBUG|^\s*$|mq-bridge-app ─|──────|Starting without UI")
    text = [line for line in path.read_text(errors="replace").splitlines() if not noise.search(line)]
    return "\n".join(text[-lines:])


def records(run_id: str, count: int) -> list:
    return [{"id": i, "run": run_id, "name": f"näme-{i}"} for i in range(1, count + 1)]


class Broker:
    def __init__(self, name: str, spec: dict, docker: bool):
        self.name, self.spec, self.docker = name, spec, docker
        self.port = None
        self.container = self.process = None
        self.unavailable = None
        self.captured = {}

    def start(self):
        exe = self.spec.get("exec")
        if exe and shutil.which(exe[0]):
            self.port = free_port()
            argv = [str(substitute(a, {"PORT": self.port})) for a in exe]
            self.process = subprocess.Popen(
                argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        elif not self.docker:
            self.unavailable = "no docker"
            return
        else:
            self.container = f"faucet-endpoints-{RUN}-{self.name}"
            argv = ["docker", "run", "-d", "--rm", "--name", self.container]
            ports = {"": self.spec["port"], **self.spec.get("ports", {})}
            # A broker that advertises its address (Kafka) must know the host port up front.
            host = free_port() if self.spec.get("fixed_port") else ""
            for port in ports.values():
                argv += ["-p", f"127.0.0.1:{host if port == self.spec['port'] else ''}:{port}"]
            for key, value in self.spec.get("env", {}).items():
                argv += ["-e", f"{key}={value}"]
            argv += [self.spec["image"], *substitute(self.spec.get("args", []), {"PORT": host})]
            started = subprocess.run(argv, capture_output=True, text=True)
            if started.returncode:
                self.unavailable = started.stderr.strip().splitlines()[-1]
                return
            for name, port in ports.items():
                mapped = subprocess.run(
                    ["docker", "port", self.container, f"{port}/tcp"],
                    capture_output=True,
                    text=True,
                ).stdout
                self.captured[name] = int(mapped.split()[0].rsplit(":", 1)[1])
            self.port = self.captured.pop("")
        if not self.wait_ready():
            self.unavailable = "broker did not become ready"
            return
        for name, command in self.spec.get("capture", {}).items():
            if self.container:
                command = ["docker", "exec", self.container, *command]
            self.captured[name] = subprocess.run(
                command, capture_output=True, text=True
            ).stdout.strip()

    def wait_ready(self) -> bool:
        pattern = self.spec.get("ready_log")
        deadline = time.time() + self.spec.get("ready_timeout", 60)
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", self.port), 1).close()
                if not (pattern and self.container):
                    return True
                logs = subprocess.run(
                    ["docker", "logs", self.container], capture_output=True, text=True
                )
                if re.search(pattern, logs.stdout + logs.stderr):
                    return True
            except OSError:
                pass
            time.sleep(0.5)
        return False

    def stop(self):
        if self.process:
            self.process.kill()
        if self.container:
            subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)


class Store:
    """What the HTTP server serves and collects, keyed by run id."""

    def __init__(self):
        self.lock = threading.Lock()
        self.seeds, self.collected = {}, {}


def http_server(store: Store) -> int:
    """GET  /records/<id>    the seed records as a JSON array
    GET  /xml/<id>        the same as <records><record>...</record></records>
    POST /graphql/<id>    {"data": {"records": [...]}}
    POST /collect/<id>    stores the posted record(s)
    GET  /collected/<id>  what was posted, as a JSON array"""

    class Handler(http.server.BaseHTTPRequestHandler):
        def reply(self, body: bytes, kind="application/json", status=200):
            self.send_response(status)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def route(self):
            parts = self.path.split("?")[0].strip("/").split("/")
            return (parts + [""])[:2]

        def do_GET(self):
            kind, key = self.route()
            with store.lock:
                seed = store.seeds.get(key, [])
                collected = list(store.collected.get(key, []))
            if kind == "records":
                self.reply(json.dumps(seed).encode())
            elif kind == "collected":
                self.reply(json.dumps(collected).encode())
            elif kind == "xml":
                rows = "".join(
                    "<record>"
                    + "".join(f"<{k}>{escape(str(v))}</{k}>" for k, v in r.items())
                    + "</record>"
                    for r in seed
                )
                self.reply(f"<records>{rows}</records>".encode(), "application/xml")
            else:
                self.reply(b"{}", status=404)

        def do_POST(self):
            kind, key = self.route()
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            if kind == "graphql":
                with store.lock:
                    seed = store.seeds.get(key, [])
                self.reply(json.dumps({"data": {"records": seed}}).encode())
            elif kind == "collect":
                try:
                    posted = json.loads(body)
                except ValueError:
                    return self.reply(b'{"error":"not json"}', status=400)
                with store.lock:
                    rows = store.collected.setdefault(key, [])
                    rows.extend(posted if isinstance(posted, list) else [posted])
                self.reply(b"{}")
            else:
                self.reply(b"{}", status=404)

        def log_message(self, *_):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_port


def found(value, run_id: str, out: list):
    """Collects every record of this run, wherever a connector nested it."""
    if isinstance(value, str) and value[:1] in "{[":
        try:
            value = json.loads(value)
        except ValueError:
            return
    if isinstance(value, dict):
        if str(value.get("run")) == run_id and "id" in value:
            out.append(value)
            return
        for inner in value.values():
            found(inner, run_id, out)
    elif isinstance(value, list):
        for inner in value:
            found(inner, run_id, out)


def read_rows(path: pathlib.Path, run_id: str) -> list:
    """This run's records in a file written by mq-bridge's `file` endpoint."""
    rows = []
    if path.exists():
        for line in path.read_text().splitlines():
            try:
                found(json.loads(line).get("payload"), run_id, rows)
            except (ValueError, AttributeError):
                continue
    return [r for r in rows if str(r["id"]).isdigit()]


def compare(rows: list, expected: list) -> str | None:
    """None when exactly the expected ids arrived, each once, else what went wrong."""
    ids = [int(r["id"]) for r in rows]
    missing = sorted(set(expected) - set(ids))
    extra = sorted(set(ids) - set(expected))
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    problems = []
    if missing:
        problems.append(f"missing {len(missing)} of {len(expected)}: {missing[:10]}")
    if extra:
        problems.append(f"unexpected: {extra[:10]}")
    if dupes:
        problems.append(f"duplicated: {dupes[:10]}")
    return "; ".join(problems) or None


class Case:
    def __init__(self, name: str, spec: dict, broker, args, workdir: pathlib.Path, store, http_port):
        self.name, self.spec, self.broker, self.args = name, spec, broker, args
        self.dir = workdir / name
        self.dir.mkdir()
        self.run_id = f"{RUN}{name.replace('_', '')}"
        self.count = spec.get("count", 10)
        self.values = {
            "ID": self.run_id,
            "DIR": self.dir,
            "HERE": HERE,
            "HTTP": f"http://127.0.0.1:{http_port}",
            "PORT": broker.port if broker else free_port(),
            **(broker.captured if broker else {}),
            **spec.get("vars", {}),
        }
        self.env = {**os.environ, **substitute(spec.get("env", {}), self.values)}
        with store.lock:
            store.seeds[self.run_id] = records(self.run_id, self.count)
        self.store = store

    def mqb(self, route: str, input_: dict, output: dict, log: str, drain=True):
        config = {"routes": {route: {"input": endpoint(input_), "output": endpoint(output)}}}
        argv = [self.args.mqb, "--no-ui", "--no-metrics", "--color", "never"]
        argv += ["--config-str", json.dumps(config)]
        if drain:
            argv.append("--drain")
        for plugin in self.args.plugin or []:
            argv += ["--plugin", plugin]
        return argv, open(self.dir / log, "w")

    def run(self, route, input_, output, log) -> int:
        argv, out = self.mqb(route, input_, output, log)
        with out:
            return subprocess.run(
                argv, stdout=out, stderr=out, env=self.env, cwd=self.dir,
                timeout=self.spec.get("timeout", 60),
            ).returncode  # fmt: skip

    def setup(self) -> str | None:
        for command in substitute(self.spec.get("setup", []), self.values):
            command = [str(arg) for arg in command]
            if self.spec.get("setup_in_broker") and self.broker and self.broker.container:
                command = ["docker", "exec", self.broker.container, *command]
            done = subprocess.run(command, capture_output=True, text=True, env=self.env, timeout=60)
            if done.returncode:
                return f"setup {command[0]} exited {done.returncode}\n{done.stderr.strip()[-800:]}"
        return None

    def write(self) -> str | None:
        seed = self.dir / "seed.jsonl"
        seed.write_text(
            "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records(self.run_id, self.count))
        )
        sink = substitute(self.spec["sink"], self.values)
        source = {"file": {"path": str(seed), "format": "raw", **self.spec.get("seed", {})}}
        for attempt in range(self.spec.get("writes", 1)):
            log = "sink.log" if attempt == 0 else f"sink{attempt + 1}.log"
            if code := self.run("write", source, sink, log):
                return f"sink route exited {code}\n{tail(self.dir / log)}"
        return None

    def read(self, received: pathlib.Path, log: str) -> str | None:
        source = substitute(self.spec["source"], self.values)
        if "resume" in self.spec:
            source.setdefault("state_path", str(self.dir / "state"))
        if code := self.run("read", source, {"file": {"path": str(received)}}, log):
            return f"source route exited {code}\n{tail(self.dir / log)}"
        return None

    def execute(self) -> tuple[bool, str]:
        if error := self.setup():
            return False, error
        received = self.dir / "received.jsonl"
        if "sink" not in self.spec:
            error = self.read(received, "source.log")
        elif self.spec.get("order") == "input_first":
            error = self.concurrently(received)
        else:
            error = self.write() or self.read(received, "source.log")
        if error:
            return False, error

        rows = read_rows(received, self.run_id)
        expected = self.spec.get("expect_ids", list(range(1, self.count + 1)))
        if problem := compare(rows, expected):
            return False, (
                f"{problem}\n-- sink.log\n{tail(self.dir / 'sink.log')}"
                f"\n-- source.log\n{tail(self.dir / 'source.log')}"
            )
        detail = f"{len(expected)} of {self.count} records"
        seed = {r["id"]: r for r in records(self.run_id, self.count)}
        for field in self.spec.get("changed", []):
            kept = [r["id"] for r in rows if r.get(field) == seed[int(r["id"])][field]]
            if kept:
                return False, f"`{field}` left unchanged in {len(kept)} records, e.g. {rows[0]}"
            detail += f", `{field}` changed"
        if "dlq_ids" in self.spec:
            if problem := compare(read_rows(self.dir / "dlq.jsonl", self.run_id), self.spec["dlq_ids"]):
                return False, f"dead-letter file: {problem}\n{tail(self.dir / 'sink.log')}"
            detail += f", {len(self.spec['dlq_ids'])} to the DLQ"
        if "resume" in self.spec:
            again = self.dir / "again.jsonl"
            if error := self.read(again, "again.log"):
                return False, f"second run: {error}"
            if problem := compare(read_rows(again, self.run_id), self.spec["resume"]):
                return False, f"second run, despite the bookmark: {problem}"
            detail += f", resumed with {len(self.spec['resume'])}"
        return True, detail

    def concurrently(self, received: pathlib.Path) -> str | None:
        """For sources that only see what arrives after they subscribed."""
        source = substitute(self.spec["source"], self.values)
        argv, log = self.mqb("read", source, {"file": {"path": str(received)}}, "source.log")
        reader = subprocess.Popen(argv, stdout=log, stderr=log, env=self.env, cwd=self.dir)
        try:
            time.sleep(self.spec.get("settle", 2))
            if reader.poll() is not None:
                return f"source ended before the sink wrote\n{tail(self.dir / 'source.log')}"
            if error := self.write():
                return error
            if code := reader.wait(self.spec.get("timeout", 60)):
                return f"source route exited {code}\n{tail(self.dir / 'source.log')}"
        except subprocess.TimeoutExpired:
            return f"source did not end\n{tail(self.dir / 'source.log')}"
        finally:
            if reader.poll() is None:
                reader.kill()
            log.close()
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("filters", nargs="*", help="run cases whose name contains one of these")
    parser.add_argument("--cases", default=HERE / "cases.toml", type=pathlib.Path)
    parser.add_argument("--mqb", default="mqb", help="the mqb binary (default: from PATH)")
    parser.add_argument("--plugin", action="append", help="libmq_bridge_faucet to load instead of a discovered one")
    parser.add_argument("--strict", action="store_true", help="a skipped case is a failure")
    parser.add_argument("--list", action="store_true", help="print the cases and exit")
    parser.add_argument("-j", "--jobs", type=int, default=4)
    parser.add_argument("--keep", action="store_true", help="keep the logs of passing runs too")
    parser.add_argument("--rmi", action="store_true", help="remove the broker images this run pulled")
    args = parser.parse_args()

    config = tomllib.loads(args.cases.read_text())
    cases = {
        name: case
        for name, case in config["cases"].items()
        if not args.filters or any(f in name for f in args.filters)
    }
    if args.list:
        for name, case in cases.items():
            print(f"{name:24} {case.get('broker', '-'):12} {case.get('xfail', '')}")
        return 0
    if not shutil.which(args.mqb):
        sys.exit(f"{args.mqb} not found; install mq-bridge-app or pass --mqb")

    needs_docker = any("broker" in c for c in cases.values())
    try:
        docker = needs_docker and subprocess.run(
            ["docker", "info"], capture_output=True, timeout=20
        ).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        docker = False

    workdir = pathlib.Path(tempfile.mkdtemp(prefix="faucet-endpoints-"))
    present = set()
    if docker and args.rmi:
        listed = subprocess.run(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"],
                                capture_output=True, text=True).stdout
        present = set(listed.split())
    brokers = {
        name: Broker(name, config["brokers"][name], docker)
        for name in sorted({c["broker"] for c in cases.values() if "broker" in c})
    }
    store = Store()
    http_port = http_server(store)
    results = {}

    def one(name):
        spec = cases[name]
        broker = brokers.get(spec.get("broker"))
        started = time.time()
        if broker and broker.unavailable:
            return name, "SKIP", 0.0, broker.unavailable
        try:
            passed, detail = Case(name, spec, broker, args, workdir, store, http_port).execute()
        except Exception as error:  # a broken case must not take the run down
            passed, detail = False, f"{type(error).__name__}: {error}"
        if "xfail" in spec:
            status = "XPASS" if passed else "XFAIL"
            detail = spec["xfail"] if not passed else f"expected to fail: {spec['xfail']}"
        else:
            status = "PASS" if passed else "FAIL"
        return name, status, time.time() - started, detail

    try:
        with concurrent.futures.ThreadPoolExecutor(args.jobs) as pool:
            list(pool.map(Broker.start, brokers.values()))
            for name, status, seconds, detail in pool.map(one, cases):
                results[name] = (status, seconds, detail)
                first, *rest = detail.splitlines() or [""]
                print(f"{status:5} {name:24} {seconds:5.1f}s  {first}", flush=True)
                for line in rest:
                    print(f"      {line}")
    finally:
        for broker in brokers.values():
            broker.stop()
        if docker and args.rmi:
            pulled = {b.spec["image"] for b in brokers.values() if b.container} - present
            if pulled:
                subprocess.run(["docker", "rmi", *sorted(pulled)], capture_output=True)

    bad = {"FAIL", "XPASS"} | ({"SKIP"} if args.strict else set())
    failed = [name for name, (status, _, _) in results.items() if status in bad]
    tally = {}
    for status, _, _ in results.values():
        tally[status] = tally.get(status, 0) + 1
    print("\n" + ", ".join(f"{n} {s}" for s, n in sorted(tally.items())))

    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as out:
            out.write("| Connector case | Result | Detail |\n| :--- | :--- | :--- |\n")
            for name, (status, _, detail) in results.items():
                first = (detail.splitlines() or [""])[0]
                out.write(f"| `{name}` | {status} | {first} |\n")

    if failed or args.keep:
        print(f"logs: {workdir}")
    else:
        shutil.rmtree(workdir, ignore_errors=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
