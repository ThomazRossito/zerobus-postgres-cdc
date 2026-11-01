#!/usr/bin/env python3
"""Mede a latência de ponta a ponta: commit no Postgres -> linha consultável no Databricks.

Lê generator/events.csv (escrito pelo simulate_ops.py), consulta a bronze no Databricks a cada
poucos segundos por um SQL warehouse e anota a primeira vez que cada (order_id, op) aparece.
Escreve measure/latency.csv e imprime p50/p95/max. Esse é o número do artigo: medido por você,
não o da documentação.

Só entram eventos gravados depois que este script começou: linhas antigas do events.csv,
de execuções anteriores, são ignoradas. Quando um pedido tem vários updates, vale o primeiro.

Pré-requisitos:
  pip install databricks-sdk
  No .env: UC_CATALOG (catálogo da bronze) e WAREHOUSE_ID (SQL warehouse).
  Autenticação pelo perfil da CLI (DATABRICKS_CONFIG_PROFILE no .env) ou pelas variáveis
  DATABRICKS_HOST / DATABRICKS_TOKEN.

Uso:
  python measure/latency.py --minutes 10
"""
import argparse
import csv
import os
import statistics
import sys
import time
from pathlib import Path

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementState
except ImportError:
    print("instale: pip install databricks-sdk"); raise

OP_MAP = {"insert": "c", "update": "u", "delete": "d"}

# o que precisa estar no .env, e onde achar cada valor
REQUIRED = {
    "UC_CATALOG": "o catálogo onde você criou retail.orders (databricks/01_tables.sql)",
    "WAREHOUSE_ID": "o ID do SQL warehouse: `databricks warehouses list --profile <perfil>`, "
                    "ou o fim do HTTP path em Connection details (/sql/1.0/warehouses/<ID>)",
}


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def read_events(path: Path, seen: set, since_ms: int) -> dict:
    """events.csv -> {(order_id, op): commit_ts_ms}: só eventos novos e ainda não observados.

    Fica a PRIMEIRA ocorrência de cada (order_id, op). Na bronze, o primeiro 'u' de um pedido é
    sempre o do primeiro update; comparar com um update posterior subestimaria a latência.
    """
    pending = {}
    if not path.exists():
        return pending
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            ts = int(row["commit_ts_ms"])
            if ts < since_ms:
                continue
            key = (int(row["order_id"]), row["op"])
            if key not in seen and key not in pending:
                pending[key] = ts
    return pending


def p95(values: list) -> float:
    return sorted(values)[int(len(values) * 0.95) - 1] if len(values) >= 20 else max(values)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=10.0)
    ap.add_argument("--poll", type=float, default=2.0, help="intervalo entre consultas (s)")
    ap.add_argument("--events", default=str(Path(__file__).resolve().parents[1] / "generator" / "events.csv"))
    ap.add_argument("--out", default=str(Path(__file__).resolve().parent / "latency.csv"))
    ap.add_argument("--env", default=str(Path(__file__).resolve().parents[1] / ".env"),
                    help="arquivo .env a usar (ex.: .env.free para outro workspace)")
    args = ap.parse_args()
    load_env(Path(args.env))

    missing = [k for k in REQUIRED if not os.environ.get(k)]
    if missing:
        print("❌ falta preencher no .env:")
        for k in missing:
            print(f"   {k}: {REQUIRED[k]}")
        return 1

    # DATABRICKS_CLIENT_ID/SECRET no .env são do service principal do Debezium. O databricks-sdk lê essas
    # mesmas variáveis e misturaria com o perfil da CLI; aqui quem consulta é você, pelo perfil.
    for k in ("DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"):
        os.environ.pop(k, None)

    catalog = os.environ["UC_CATALOG"]
    warehouse = os.environ["WAREHOUSE_ID"]
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE") or None
    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    table = f"`{catalog}`.retail.orders"
    print(f"workspace: {w.config.host} (perfil: {profile or 'padrão da CLI'})")

    since_ms = int(time.time() * 1000)
    seen: set = set()
    latencies = []
    deadline = time.time() + args.minutes * 60
    out_new = not Path(args.out).exists()
    with open(args.out, "a", newline="") as fh:
        out = csv.writer(fh)
        if out_new:
            out.writerow(["order_id", "op", "commit_ts_ms", "seen_ts_ms", "latency_ms"])
        print(f"medindo contra {table} a cada {args.poll}s por {args.minutes} min (só eventos a partir de agora)")
        while time.time() < deadline:
            pending = read_events(Path(args.events), seen, since_ms)
            if pending:
                # só os 500 mais recentes por consulta, para a query ficar leve
                keys = sorted(pending, key=lambda k: pending[k])[-500:]
                ids = ",".join(str(k[0]) for k in keys)
                sql = (f"SELECT order_id, __op FROM {table} "
                       f"WHERE order_id IN ({ids}) AND __op IN ('c','u','d') GROUP BY order_id, __op")
                res = w.statement_execution.execute_statement(
                    warehouse_id=warehouse, statement=sql, wait_timeout="30s",
                    on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL)
                state = res.status.state if res.status else None
                if state == StatementState.FAILED:
                    err = res.status.error.message if res.status.error else "sem detalhe"
                    print(f"❌ a consulta falhou: {err}")
                    print(f"   confira se {table} existe (databricks/01_tables.sql) e se você tem SELECT nela")
                    return 1
                if state != StatementState.SUCCEEDED:
                    print(f"{time.strftime('%H:%M:%S')} consulta não terminou em 30 s ({state}); "
                          f"o warehouse pode estar ligando, tentando de novo")
                    time.sleep(args.poll)
                    continue
                now_ms = int(time.time() * 1000)
                rows = (res.result.data_array or []) if res.result else []
                found = {(int(r[0]), r[1]) for r in rows}
                matched = 0
                for key in keys:
                    oid, op = key
                    if (oid, OP_MAP[op]) in found:
                        lat = now_ms - pending[key]
                        seen.add(key)
                        latencies.append(lat)
                        out.writerow([oid, op, pending[key], now_ms, lat])
                        matched += 1
                fh.flush()
                if latencies:
                    # p50 acumulado inclui o começo; o dos últimos 200 mostra o regime de agora
                    print(f"{time.strftime('%H:%M:%S')} observados={len(latencies)} "
                          f"p50={statistics.median(latencies):.0f}ms "
                          f"p50_ultimos200={statistics.median(latencies[-200:]):.0f}ms "
                          f"p95={p95(latencies):.0f}ms max={max(latencies)}ms pendentes={len(pending) - matched}")
                else:
                    print(f"{time.strftime('%H:%M:%S')} aguardando: {len(pending)} eventos ainda não chegaram na bronze")
            time.sleep(args.poll)

    if latencies:
        print(f"\nresumo: n={len(latencies)} p50={statistics.median(latencies):.0f}ms "
              f"p95={p95(latencies):.0f}ms max={max(latencies)}ms "
              f"p50_ultimos200={statistics.median(latencies[-200:]):.0f}ms")
        print("atenção: a resolução é o intervalo de polling; latência real <= medida.")
    else:
        print("\nnenhum evento observado: o Debezium Server está rodando e o simulate_ops.py também?")
    return 0


if __name__ == "__main__":
    sys.exit(main())