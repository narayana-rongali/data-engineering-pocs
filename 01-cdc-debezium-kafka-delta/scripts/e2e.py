"""End-to-end proof against real SQL Server, Debezium and Kafka. Needs Docker. Runs in CI."""
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from cdc.consumer import run  # noqa: E402
from cdc.delta_sink import read_rows  # noqa: E402

TABLE = ROOT / "output" / "e2e_orders"
SQLCMD = "/opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'LocalOnly!Passw0rd'"
TOPIC = "shop.retail.dbo.orders"


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True)


def sql(statement):
    compose("exec", "-T", "sqlserver", "bash", "-c", f'{SQLCMD} -d retail -b -Q "{statement}"')


def wait_for_connect():
    for _ in range(90):
        try:
            with urllib.request.urlopen("http://localhost:8083/connectors", timeout=3):
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            time.sleep(2)
    raise TimeoutError("Kafka Connect did not start")


def register_connector():
    request = urllib.request.Request(
        "http://localhost:8083/connectors",
        data=(ROOT / "connector.json").read_bytes(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        print("connector registered:", response.status)


def main():
    shutil.rmtree(TABLE, ignore_errors=True)
    compose("up", "-d", "--wait", "sqlserver")
    compose("up", "-d", "kafka", "connect")
    compose("exec", "-T", "sqlserver", "bash", "-c", f"{SQLCMD} -b -i /sql/seed.sql")
    wait_for_connect()
    register_connector()

    # Snapshot of the three seed rows.
    print(run("localhost:29092", TOPIC, TABLE, run_seconds=45))
    assert [row["order_id"] for row in read_rows(TABLE)] == [1, 2, 3], read_rows(TABLE)

    # Change data: one insert, one update, one delete.
    sql("INSERT INTO dbo.orders VALUES (4, 12, 102, 9, 'new')")
    sql("UPDATE dbo.orders SET quantity = 50, status = 'shipped' WHERE order_id = 1")
    sql("DELETE FROM dbo.orders WHERE order_id = 2")
    print(run("localhost:29092", TOPIC, TABLE, run_seconds=45))

    rows = {row["order_id"]: row for row in read_rows(TABLE)}
    print(json.dumps(rows, indent=2))
    assert sorted(rows) == [1, 3, 4], rows
    assert rows[1]["quantity"] == 50 and rows[1]["status"] == "shipped", rows
    print("E2E PASSED: insert, update and delete all reached Delta")


if __name__ == "__main__":
    main()
    # Every assertion has passed by here. Normal interpreter teardown can abort inside native
    # library threads (seen in CI: "terminate called without an active exception"), which would
    # turn a passing run into exit code 134, so exit directly instead. Failures still raise above.
    sys.stdout.flush()
    os._exit(0)
