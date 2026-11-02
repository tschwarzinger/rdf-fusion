#!/usr/bin/env python3
"""Integration tests that use the `rdf-fusion` binary as a user would.

Runs every CLI command against the demo data in `examples/data` and smoke tests the SPARQL
endpoint of `rdf-fusion serve` over HTTP. The tests check response bodies, not only status codes,
as a broken result stream still produces a `200 OK`.

Only uses the Python standard library.

Usage:
    python3 cli/tests/integration.py --bin target/debug/rdf-fusion [unittest args]

The binary can also be set via the `RDF_FUSION_BIN` environment variable. Usually, this script is
invoked via `just test-integration`, which builds the binary first.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BIN: Path
DEMO_DATA = Path(__file__).resolve().parent.parent.parent / "examples" / "data"

# The number of quads in `spiderman.ttl` (all in the default graph).
SPIDERMAN_QUADS = 7
# The number of quads in `paris-graph.trig` (all in a single named graph).
PARIS_QUADS = 8
PARIS_GRAPH = "http://www.wikidata.org/wiki/Special:EntityData/Q90"

SPIDERMAN = "http://example.org/#spiderman"
COUNT_ALL = "SELECT (COUNT(*) AS ?count) WHERE { { ?s ?p ?o } UNION { GRAPH ?g { ?s ?p ?o } } }"
SELECT_NAMES = (
    f"SELECT ?name WHERE {{ <{SPIDERMAN}> <http://xmlns.com/foaf/0.1/name> ?name }} "
    "ORDER BY ?name"
)

COMMAND_TIMEOUT_S = 120
SERVER_STARTUP_TIMEOUT_S = 60

# Ignore proxy settings of the environment, as the server runs locally.
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def demo_data(file: str) -> str:
    return str(DEMO_DATA / file)


def run(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Runs the binary with the given arguments."""
    result = subprocess.run(
        [str(BIN), *args],
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT_S,
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"`rdf-fusion {' '.join(args)}` failed with exit code {result.returncode}\n"
            f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
        )
    return result


class Storage:
    """A storage that the CLI operates on."""

    def __init__(self, storage_type: str, location: str):
        self.storage_type = storage_type
        self.location = location

    @staticmethod
    def delta(directory: str) -> "Storage":
        return Storage("delta-quads", Path(directory, "db").as_uri() + "/")

    @staticmethod
    def parquet(location: str) -> "Storage":
        return Storage("parquet", location)

    def args(self, *args: str) -> list[str]:
        return ["--storage-type", self.storage_type, "--location", self.location, *args]

    def run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return run(self.args(*args), check=check)

    def query_count(self, query: str) -> int:
        """Runs the `query` command and returns the value of a single-solution result."""
        stdout = self.run("query", query).stdout
        lines = [line for line in stdout.splitlines() if line.strip()]
        assert len(lines) == 2, f"expected a header and a single solution, got:\n{stdout}"
        return int(lines[1].strip())


class Server:
    """A running `rdf-fusion serve` process."""

    def __init__(self, storage: Storage):
        self.port = free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.query_url = f"{self.base_url}/repositories/default/query"
        self.data_url = f"{self.base_url}/repositories/default/data"
        self.log_file = tempfile.NamedTemporaryFile(prefix="rdf-fusion-serve-", suffix=".log")
        self.process = subprocess.Popen(
            [str(BIN), *storage.args("serve", "--bind", f"127.0.0.1:{self.port}")],
            stdin=subprocess.DEVNULL,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
        )

    def __enter__(self) -> "Server":
        try:
            self.wait_until_ready()
        except BaseException:
            self.stop()
            raise
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    def stop(self) -> None:
        self.process.kill()
        self.process.wait()
        self.log_file.close()

    def wait_until_ready(self) -> None:
        deadline = time.monotonic() + SERVER_STARTUP_TIMEOUT_S
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(
                    f"`rdf-fusion serve` exited with code {self.process.returncode}\n{self.log()}"
                )
            try:
                status, _ = self.get_query("ASK {}", "application/sparql-results+json")
                if status == 200:
                    return
            except (urllib.error.URLError, ConnectionError):
                pass
            time.sleep(0.1)
        raise AssertionError(f"`rdf-fusion serve` did not become ready\n{self.log()}")

    def log(self) -> str:
        return "--- server log ---\n" + Path(self.log_file.name).read_text(errors="replace")

    def request(
        self,
        url: str,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, str]:
        """Sends a request and returns the status and the body of the response."""
        request = urllib.request.Request(url, data=data, headers=headers or {})
        try:
            with HTTP.open(request, timeout=COMMAND_TIMEOUT_S) as response:
                return response.status, response.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode(errors="replace")

    def get_query(self, query: str, accept: str) -> tuple[int, str]:
        url = f"{self.query_url}?{urllib.parse.urlencode({'query': query})}"
        return self.request(url, headers={"Accept": accept})

    def post_query(self, body: bytes, content_type: str, accept: str) -> tuple[int, str]:
        headers = {"Content-Type": content_type, "Accept": accept}
        return self.request(self.query_url, data=body, headers=headers)


def free_port() -> int:
    """Asks the OS for a free port. Another process could claim it before the server binds it,
    but this is unlikely in practice."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def json_bindings(body: str) -> list:
    try:
        return json.loads(body)["results"]["bindings"]
    except (ValueError, KeyError) as e:
        raise AssertionError(f"invalid SPARQL JSON results ({e!r}): {body!r}") from e


class TempDirTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = tempfile.mkdtemp(prefix="rdf-fusion-it-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def loaded_delta(self) -> Storage:
        """Loads all demo data into a fresh Delta Lake storage."""
        storage = Storage.delta(self.dir)
        storage.run("load", demo_data("spiderman.ttl"), demo_data("paris-graph.trig"))
        return storage


class CommandTests(TempDirTestCase):
    """Runs every CLI command against the demo data."""

    def test_version_and_help(self):
        for arg in ["--version", "--help"]:
            self.assertTrue(run([arg]).stdout.strip())

    def test_load_and_query_delta(self):
        storage = self.loaded_delta()

        self.assertEqual(storage.query_count(COUNT_ALL), SPIDERMAN_QUADS + PARIS_QUADS)
        names = storage.run("query", SELECT_NAMES).stdout
        self.assertIn('"Spiderman"', names)
        self.assertIn('"Человек-паук"@ru', names)

    def test_query_explain_analyze(self):
        storage = self.loaded_delta()

        stdout = storage.run("query", "--explain", "--analyze", COUNT_ALL).stdout

        self.assertIn("Execution Plan:", stdout)

    def test_optimize_keeps_data(self):
        storage = self.loaded_delta()

        storage.run("optimize")

        self.assertEqual(storage.query_count(COUNT_ALL), SPIDERMAN_QUADS + PARIS_QUADS)

    def test_dump_round_trip(self):
        storage = self.loaded_delta()
        dump = Path(self.dir, "dump.nq")

        storage.run("dump", str(dump), "--format", "nq", "--sort-by", "GSPO")

        self.assertEqual(len(dump.read_text().splitlines()), SPIDERMAN_QUADS + PARIS_QUADS)
        # Loading the dump into a new storage must yield the same data.
        other = Storage.delta(tempfile.mkdtemp(dir=self.dir))
        other.run("load", str(dump))
        self.assertEqual(other.query_count(COUNT_ALL), SPIDERMAN_QUADS + PARIS_QUADS)

    def test_dump_graph_to_triples(self):
        storage = self.loaded_delta()
        dump = Path(self.dir, "paris.nt")

        storage.run(
            "dump", str(dump), "--graph", PARIS_GRAPH, "--triple-fallback", "ignore"
        )

        self.assertEqual(len(dump.read_text().splitlines()), PARIS_QUADS)

    def test_query_demo_parquet(self):
        storage = Storage.parquet(demo_data("spiderman.parquet"))

        self.assertEqual(storage.query_count(COUNT_ALL), SPIDERMAN_QUADS)

    def test_load_and_query_parquet(self):
        storage = Storage.parquet(str(Path(self.dir, "spiderman.parquet")))

        storage.run("load", demo_data("spiderman.ttl"))

        self.assertEqual(storage.query_count(COUNT_ALL), SPIDERMAN_QUADS)

    def test_invalid_query_fails(self):
        storage = Storage.parquet(demo_data("spiderman.parquet"))

        result = storage.run("query", "SELECT WHERE {", check=False)

        self.assertNotEqual(result.returncode, 0)


class ServeTests(TempDirTestCase):
    """Smoke tests the SPARQL endpoint of `rdf-fusion serve` over HTTP."""

    def test_serve_delta(self):
        with Server(self.loaded_delta()) as server:
            self.smoke_test_endpoint(server)

            # Named graphs are queryable.
            status, body = server.get_query(
                f"SELECT (COUNT(*) AS ?c) WHERE {{ GRAPH <{PARIS_GRAPH}> {{ ?s ?p ?o }} }}",
                "text/csv",
            )
            self.assertEqual(status, 200, f"{body}\n{server.log()}")
            self.assertEqual(body.splitlines()[1:], [str(PARIS_QUADS)])

    def test_serve_parquet(self):
        with Server(Storage.parquet(demo_data("spiderman.parquet"))) as server:
            self.smoke_test_endpoint(server)

    def test_serve_memory_with_data_upload(self):
        with Server(Storage("delta-quads", "memory:///")) as server:
            status, body = server.request(
                server.data_url,
                data=Path(demo_data("spiderman.ttl")).read_bytes(),
                headers={"Content-Type": "text/turtle"},
            )
            self.assertIn(status, (200, 204), f"{body}\n{server.log()}")

            self.smoke_test_endpoint(server)

    def smoke_test_endpoint(self, server: Server) -> None:
        """Runs the same set of requests against a server that serves `spiderman.ttl` in its
        default graph."""
        with self.subTest("SELECT in all result formats"):
            self.select_in_all_formats(server)
        with self.subTest("SELECT via POST"):
            self.select_via_post(server)
        with self.subTest("SELECT with many solutions"):
            self.select_many_solutions(server)
        with self.subTest("ASK"):
            self.ask(server)
        with self.subTest("CONSTRUCT and DESCRIBE"):
            self.construct_and_describe(server)
        with self.subTest("invalid requests"):
            self.invalid_requests(server)

    def select_in_all_formats(self, server: Server) -> None:
        for accept in [
            "application/sparql-results+json",
            "application/sparql-results+xml",
            "text/csv",
            "text/tab-separated-values",
        ]:
            status, body = server.get_query(SELECT_NAMES, accept)
            self.assertEqual(status, 200, f"{accept}: {body}\n{server.log()}")
            self.assertIn("Spiderman", body, accept)
            self.assertIn("Человек-паук", body, accept)

        _, body = server.get_query(SELECT_NAMES, "application/sparql-results+json")
        self.assertEqual(len(json_bindings(body)), 2, body)

    def select_via_post(self, server: Server) -> None:
        accept = "application/sparql-results+json"
        for content_type, body in [
            (
                "application/x-www-form-urlencoded",
                urllib.parse.urlencode({"query": SELECT_NAMES}).encode(),
            ),
            ("application/sparql-query", SELECT_NAMES.encode()),
        ]:
            status, response = server.post_query(body, content_type, accept)
            self.assertEqual(status, 200, f"{content_type}: {response}")
            self.assertEqual(len(json_bindings(response)), 2, f"{content_type}: {response}")

    def select_many_solutions(self, server: Server) -> None:
        """Produces more solutions than a single small chunk to exercise streaming."""
        status, body = server.get_query(
            "SELECT * WHERE { ?a ?b ?c . ?d ?e ?f . ?g ?h ?i }",
            "application/sparql-results+json",
        )
        self.assertEqual(status, 200, f"{body}\n{server.log()}")
        self.assertEqual(len(json_bindings(body)), SPIDERMAN_QUADS**3)

    def ask(self, server: Server) -> None:
        for query, expected in [
            (f"ASK {{ <{SPIDERMAN}> ?p ?o }}", True),
            ("ASK { <http://example.org/#batman> ?p ?o }", False),
        ]:
            status, body = server.get_query(query, "application/sparql-results+json")
            self.assertEqual(status, 200, f"{body}\n{server.log()}")
            self.assertIs(json.loads(body)["boolean"], expected, body)

    def construct_and_describe(self, server: Server) -> None:
        status, body = server.get_query(
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", "application/n-triples"
        )
        self.assertEqual(status, 200, f"{body}\n{server.log()}")
        self.assertEqual(len(body.splitlines()), SPIDERMAN_QUADS, body)

        status, body = server.get_query(f"DESCRIBE <{SPIDERMAN}>", "text/turtle")
        self.assertEqual(status, 200, f"{body}\n{server.log()}")
        # Only checks that a description is returned, as its content is implementation-defined.
        self.assertIn(SPIDERMAN, body)

    def invalid_requests(self, server: Server) -> None:
        status, _ = server.get_query("SELECT WHERE {", "application/sparql-results+json")
        self.assertEqual(status, 400)

        status, _ = server.get_query(SELECT_NAMES, "image/png")
        self.assertEqual(status, 406)


def main() -> None:
    global BIN
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--bin",
        default=os.environ.get("RDF_FUSION_BIN"),
        help="Path to the rdf-fusion binary (default: $RDF_FUSION_BIN)",
    )
    args, unittest_args = parser.parse_known_args()
    if not args.bin:
        parser.error("the binary is required: pass --bin or set RDF_FUSION_BIN")
    BIN = Path(args.bin).resolve()
    if not BIN.is_file():
        parser.error(f"binary not found: {BIN}")

    print(f"Testing {BIN}", file=sys.stderr)
    unittest.main(argv=[sys.argv[0], *unittest_args], verbosity=2)


if __name__ == "__main__":
    main()
