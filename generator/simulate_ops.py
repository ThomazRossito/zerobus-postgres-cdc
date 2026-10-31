#!/usr/bin/env python3
"""Simula a operação da loja em tempo real: pedidos novos, mudanças de status,
cancelamentos (delete) e ajustes de estoque. É o que o Debezium captura e o Zerobus leva ao Databricks.

Uso:
  python generator/simulate_ops.py --rate 20 --minutes 10
    --rate     operações por segundo
    --minutes  duração: roda rate x minutes x 60 operações; 0 = até Ctrl+C

Grava generator/events.csv com (op, order_id, commit_ts_ms) para o measure/latency.py medir
quanto tempo cada pedido leva do commit no Postgres até ficar consultável no Databricks.
A barra de progresso (tqdm) mostra as operações feitas e, ao lado, quantas de cada tipo.
"""
import argparse
import csv
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from tqdm import tqdm

NEXT_STATUS = {"NEW": "PICKING", "PICKING": "SHIPPED", "SHIPPED": "DELIVERED"}
STOP = False

# com total (--minutes > 0): percentual e tempo restante; sem total (--minutes 0): só contagem e taxa
BAR_FORMAT = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]"
BAR_FORMAT_SEM_TOTAL = "{desc}: {n_fmt} op [{elapsed}, {rate_fmt}{postfix}]"


def _stop(*_):
    global STOP
    STOP = True


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def conninfo() -> str:
    return (f"host={os.environ.get('PG_HOST', 'localhost')} port={os.environ.get('PG_PORT', '5432')} "
            f"dbname={os.environ.get('PG_DBNAME', 'retail_demo')} "
            f"user={os.environ.get('PG_USER_ADMIN', 'postgres')} password={os.environ.get('PG_PASSWORD_ADMIN', '')}")


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=10.0)
    ap.add_argument("--minutes", type=float, default=5.0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--events", default=str(Path(__file__).resolve().parent / "events.csv"))
    args = ap.parse_args()
    load_env(Path(__file__).resolve().parents[1] / ".env")
    random.seed(args.seed)
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    interval = 1.0 / max(args.rate, 0.1)
    total = round(args.rate * args.minutes * 60) if args.minutes > 0 else None
    counts = {"insert": 0, "update": 0, "delete": 0, "inventory": 0}

    with psycopg.connect(conninfo(), autocommit=True) as conn, conn.cursor() as cur, \
            open(args.events, "a", newline="") as fh:
        ev = csv.writer(fh)
        if fh.tell() == 0:
            ev.writerow(["op", "order_id", "commit_ts_ms"])
        cur.execute("SELECT store_id FROM retail.stores")
        stores = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT sku, unit_price FROM retail.products")
        products = cur.fetchall()
        print(f"simulando ~{args.rate}/s em {len(stores)} lojas e {len(products)} produtos. Ctrl+C para parar.")

        pbar = tqdm(total=total, desc="operações", unit="op",
                    bar_format=BAR_FORMAT if total else BAR_FORMAT_SEM_TOTAL)
        proximo = time.monotonic()
        while not STOP and (total is None or pbar.n < total):
            r = random.random()
            now = utcnow()
            op = None
            if r < 0.55:  # pedido novo
                sku, price = random.choice(products)
                cur.execute("INSERT INTO retail.orders (store_id, sku, qty, unit_price, status, created_at, updated_at) "
                            "VALUES (%s, %s, %s, %s, 'NEW', %s, %s) RETURNING order_id",
                            (random.choice(stores), sku, random.randint(1, 12), price, now, now))
                oid = cur.fetchone()[0]
                ev.writerow(["insert", oid, int(time.time() * 1000)])
                op = "insert"
            elif r < 0.85:  # avança status de um pedido aberto recente
                cur.execute("SELECT order_id, status FROM retail.orders WHERE status IN ('NEW','PICKING','SHIPPED') "
                            "ORDER BY order_id DESC LIMIT 200")
                rows = cur.fetchall()
                if rows:
                    oid, st = random.choice(rows)
                    cur.execute("UPDATE retail.orders SET status=%s, updated_at=%s WHERE order_id=%s",
                                (NEXT_STATUS[st], now, oid))
                    ev.writerow(["update", oid, int(time.time() * 1000)])
                    op = "update"
            elif r < 0.92:  # cancelamento: delete físico para o CDC mostrar o 'd'
                cur.execute("SELECT order_id FROM retail.orders WHERE status='NEW' ORDER BY order_id DESC LIMIT 100")
                rows = cur.fetchall()
                if rows:
                    oid = random.choice(rows)[0]
                    cur.execute("DELETE FROM retail.orders WHERE order_id=%s", (oid,))
                    ev.writerow(["delete", oid, int(time.time() * 1000)])
                    op = "delete"
            else:  # movimento de estoque
                sku, _ = random.choice(products)
                cur.execute("UPDATE retail.inventory SET on_hand = GREATEST(on_hand + %s, 0), updated_at=%s "
                            "WHERE store_id=%s AND sku=%s", (random.randint(-15, 40), now, random.choice(stores), sku))
                op = "inventory"
            fh.flush()

            if op:
                counts[op] += 1
                pbar.set_postfix({"ins": counts["insert"], "upd": counts["update"],
                                  "del": counts["delete"], "est": counts["inventory"]}, refresh=False)
                pbar.update(1)

            # ritmo fixo: agenda a próxima operação no relógio, para o --rate valer de verdade
            proximo += interval
            espera = proximo - time.monotonic()
            if espera > 0:
                time.sleep(espera)
            elif espera < -1:  # atrasou mais de 1 s (ex.: o Mac dormiu): retoma sem disparar em rajada
                proximo = time.monotonic()
        pbar.close()

    print("fim:", counts)
    return 0


if __name__ == "__main__":
    sys.exit(main())