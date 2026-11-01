"""Teste de fumaça do Zerobus Ingest: manda N registros direto numa tabela, sem Debezium.

Serve para conferir um workspace novo (ex.: Free Edition) com o mesmo .env do projeto,
ou com outro arquivo (--env .env.free), sem mexer no .env principal.

  pip install databricks-zerobus-ingest-sdk
  python tools/zerobus_smoke.py --env .env.free                        # 10 registros em <UC_CATALOG>.retail.inventory
  python tools/zerobus_smoke.py --env .env.free --table retail.orders  # ou na orders (order_id negativo)
  python tools/zerobus_smoke.py --env .env.free --n 100

Depois, no SQL (uns 5 s depois do ack):
  SELECT count(*) FROM <UC_CATALOG>.retail.inventory WHERE __op = 'smoke';

Os registros ficam marcados com __op = 'smoke' (na orders, order_id negativo) para apagar depois:
  DELETE FROM <UC_CATALOG>.retail.inventory WHERE __op = 'smoke';

API do SDK conforme o README do databricks-zerobus-ingest-sdk (consultado em 01/11/2026).
"""
import argparse
import os
import sys
import time
from pathlib import Path

REQUIRED = ["ZEROBUS_SERVER_ENDPOINT", "DATABRICKS_WORKSPACE_URL",
            "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET", "UC_CATALOG"]


def load_env(path: Path) -> dict:
    """Lê um arquivo .env (KEY=VALUE) e devolve só as chaves que o teste usa."""
    env = {}
    if not path.exists():
        sys.exit(f"não achei {path}")
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def diagnose(err: str) -> str:
    e = err.lower()
    if "default storage" in e or "4024" in e or "unsupported table kind" in e:
        return ("a tabela está em default storage (o storage padrão da Databricks). Em julho/2026 o Zerobus "
                "rejeitava esse tipo de tabela, que é o único que o Free Edition oferece.")
    if "authorization_details" in e or "not authorized" in e or "401" in e:
        return ("falta permissão: confira USE CATALOG, USE SCHEMA e MODIFY + SELECT direto na tabela "
                "(databricks/02_grants.sql) e o entitlement Workspace access do service principal.")
    if "invalid_client" in e or "client_secret" in e or "unauthorized_client" in e:
        return "client id ou secret errados, ou o secret expirou."
    if "unavailable" in e or "dns" in e or "name resolution" in e or "connect" in e:
        return ("não alcançou o endpoint: confira o workspace id e a região no ZEROBUS_SERVER_ENDPOINT "
                "(SELECT current_metastore() mostra <cloud>:<região>:<uuid>).")
    if "schema" in e or "field" in e or "column" in e:
        return "o registro não bate com as colunas da tabela: ela foi criada com databricks/01_tables.sql?"
    return "erro não catalogado aqui; cole a mensagem inteira que eu olho."


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", default=".env", help="arquivo .env a usar (padrão: .env do projeto)")
    ap.add_argument("--table", default="retail.inventory", help="schema.tabela dentro de UC_CATALOG")
    ap.add_argument("--n", type=int, default=10, help="quantos registros mandar")
    args = ap.parse_args()

    env = load_env(Path(args.env))
    missing = [k for k in REQUIRED if not env.get(k)]
    if missing:
        sys.exit(f"faltam no {args.env}: {', '.join(missing)}")

    try:
        from zerobus.sdk.sync import ZerobusSdk
        from zerobus.sdk.shared import TableProperties
    except ImportError:
        sys.exit("SDK não instalado. Rode: pip install databricks-zerobus-ingest-sdk")

    table = f"{env['UC_CATALOG']}.{args.table}"
    endpoint = env["ZEROBUS_SERVER_ENDPOINT"]
    print(f"workspace: {env['DATABRICKS_WORKSPACE_URL']}")
    print(f"endpoint:  {endpoint}")
    print(f"tabela:    {table}")
    print(f"enviando {args.n} registros com __op = 'smoke'...")

    t0 = time.perf_counter()
    try:
        sdk = ZerobusSdk(endpoint, env["DATABRICKS_WORKSPACE_URL"])
        stream = sdk.create_stream(env["DATABRICKS_CLIENT_ID"], env["DATABRICKS_CLIENT_SECRET"],
                                   TableProperties(table))
    except Exception as exc:  # noqa: BLE001
        print(f"\n❌ não abriu o stream: {exc}\n→ {diagnose(str(exc))}")
        return 1

    now_ms = int(time.time() * 1000)
    meta = {"__op": "smoke", "__ts_ms": now_ms, "__source_ts_ms": now_ms, "__lsn": -1, "__deleted": "false"}
    try:
        for i in range(args.n):
            if args.table.endswith(".orders"):
                rec = {"order_id": -(i + 1), "store_id": 1, "sku": f"SMOKE-{i:03d}", "qty": 1,
                       "unit_price": "9.99", "status": "SMOKE",
                       "created_at": now_ms * 1000, "updated_at": now_ms * 1000}   # TIMESTAMP em JSON = epoch em µs
            else:
                rec = {"store_id": 1, "sku": f"SMOKE-{i:03d}", "on_hand": i, "reorder_pt": 0,
                       "updated_at": now_ms * 1000}
            stream.ingest_record_offset({**rec, **meta})
        stream.flush()                            # espera o ack: durável, ainda não consultável
    except Exception as exc:  # noqa: BLE001
        print(f"\n❌ falhou ao enviar: {exc}\n→ {diagnose(str(exc))}")
        return 1
    finally:
        try:
            stream.close()
        except Exception:  # noqa: BLE001
            pass

    print(f"✅ ack recebido em {time.perf_counter() - t0:.1f} s (inclui a conexão e o OAuth).")
    print("   A linha aparece na consulta uns 5 s depois:")
    print(f"   SELECT count(*) FROM {table} WHERE __op = 'smoke';   -- esperado: {args.n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
