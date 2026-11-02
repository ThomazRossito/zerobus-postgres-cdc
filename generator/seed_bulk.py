#!/usr/bin/env python3
"""Carga em massa no PostgreSQL local (lojas, produtos, estoque e pedidos históricos).

Uso:
  python generator/seed_bulk.py --stores 40 --products 400 --orders 200000   # 1ª vez: cria tudo
  python generator/seed_bulk.py --orders 5000                                # depois: append de pedidos

Primeira execução (tabelas vazias): cria lojas, produtos, estoque (lojas x produtos) e pedidos.
Execuções seguintes: append. Lojas, produtos e estoque ficam como estão e entram só pedidos novos,
com order_id seguindo a sequência; o Debezium manda cada um como insert (__op = c) para o Zerobus.

--truncate apaga tudo e recomeça do zero. Não use com o Debezium rodando: o conector do Postgres
ignora TRUNCATE por padrão (skipped.operations=t), então a bronze não é limpa, e os order_id
recomeçam do 1, misturando pedidos novos com antigos no Databricks.

Lê a conexão do .env (PG_HOST, PG_PORT, PG_DBNAME, PG_USER_ADMIN, PG_PASSWORD_ADMIN).
Usa COPY (psycopg 3) para a carga ser rápida mesmo com centenas de milhares de linhas.
Timestamps em UTC sem fuso, porque o sink Zerobus mapeia timestamp (sem tz) para TIMESTAMP no Delta.

Mostra uma barra de progresso (tqdm) por tabela. A barra conta as linhas enviadas no COPY;
o commit é um só, no fim: se algo falhar no meio, nada fica gravado.
"""
import argparse
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from tqdm import tqdm

CATEGORIES = ["Mercearia", "Bebidas", "Laticínios", "Congelados", "Higiene", "Limpeza", "Hortifruti", "Padaria"]
PERISHABLE = {"Laticínios", "Congelados", "Hortifruti", "Padaria"}
CITIES = [("São Paulo", "SP"), ("Campinas", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"),
          ("Curitiba", "PR"), ("Joinville", "SC"), ("Porto Alegre", "RS"), ("Salvador", "BA"),
          ("Recife", "PE"), ("Goiânia", "GO"), ("Brasília", "DF"), ("Fortaleza", "CE")]
# pesos do status nos pedidos históricos: 70% entregue, 12% enviado, 8% separando, 5% novo, 5% cancelado
STATUS_HIST = ["DELIVERED"] * 70 + ["SHIPPED"] * 12 + ["PICKING"] * 8 + ["NEW"] * 5 + ["CANCELLED"] * 5

# mesmo formato do script de MySQL: percentual, contagem, tempo decorrido/restante e taxa
BAR_FORMAT = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"


def progress(iterable, desc: str, total: int):
    """Envolve o iterável numa barra tqdm, com o nome da tabela alinhado à esquerda."""
    return tqdm(iterable, total=total, desc=f"{desc:<9}", unit="reg", bar_format=BAR_FORMAT)


def load_env(path: Path) -> None:
    # lê o .env sem dependência extra. setdefault: variável já exportada no terminal vence a do arquivo
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def conninfo() -> str:
    # usuário admin (dono das tabelas), não o dbz: o dbz do Debezium só lê (SELECT + REPLICATION)
    return (f"host={os.environ.get('PG_HOST', 'localhost')} port={os.environ.get('PG_PORT', '5432')} "
            f"dbname={os.environ.get('PG_DBNAME', 'retail_demo')} "
            f"user={os.environ.get('PG_USER_ADMIN', 'postgres')} password={os.environ.get('PG_PASSWORD_ADMIN', '')}")


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def main() -> int:
    ap = argparse.ArgumentParser(description="Carga em massa (1ª vez) ou append de pedidos (execuções seguintes).")
    ap.add_argument("--stores", type=int, default=40, help="lojas (só na primeira carga)")
    ap.add_argument("--products", type=int, default=400, help="produtos (só na primeira carga)")
    ap.add_argument("--orders", type=int, default=200_000, help="pedidos a inserir")
    ap.add_argument("--days", type=int, default=90, help="janela histórica dos pedidos")
    ap.add_argument("--seed", type=int, default=None,
                    help="semente do random (padrão: 42 na primeira carga, aleatória no append)")
    ap.add_argument("--truncate", action="store_true",
                    help="apaga tudo e recomeça do zero; não use com o Debezium rodando")
    ap.add_argument("--sem-fk", action="store_true",
                    help="pula a checagem de chave estrangeira nesta sessão (session_replication_role=replica, "
                         "precisa de superusuário). Para cargas grandes: sem isso o Postgres confere loja e "
                         "produto de cada pedido no FIM do COPY, depois da barra chegar a 100%%")
    args = ap.parse_args()

    load_env(Path(__file__).resolve().parents[1] / ".env")
    t0 = utcnow()
    inicio = time.perf_counter()

    # sem autocommit: a carga inteira é uma transação só, com commit no fim
    with psycopg.connect(conninfo()) as conn, conn.cursor() as cur:
        if args.sem_fk:
            # os pedidos usam lojas e produtos lidos do próprio banco, então a checagem é redundante.
            # replica desliga os gatilhos da sessão, e a FK do Postgres é feita por gatilho. O WAL é
            # gravado igual: o Debezium vê todas as linhas.
            cur.execute("SET session_replication_role = replica")
            print("⚠️  --sem-fk: chaves estrangeiras não serão conferidas nesta carga")
        if args.truncate:
            cur.execute("TRUNCATE retail.orders, retail.inventory, retail.products, retail.stores RESTART IDENTITY CASCADE")

        cur.execute("SELECT (SELECT count(*) FROM retail.stores), (SELECT count(*) FROM retail.products)")
        n_stores, n_products = cur.fetchone()
        if (n_stores > 0) != (n_products > 0):
            print("❌ retail.stores e retail.products estão inconsistentes (uma vazia e a outra não). "
                  "Rode com --truncate, com o Debezium parado.")
            return 1
        append = n_stores > 0
        # primeira carga reproduzível (42); no append, semente aleatória para não repetir o mesmo lote
        random.seed(args.seed if args.seed is not None else (None if append else 42))

        if append:
            cur.execute("SELECT store_id FROM retail.stores ORDER BY store_id")
            store_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT sku, unit_price FROM retail.products ORDER BY sku")
            skus = cur.fetchall()
            print(f"↪ append: reaproveitando {len(store_ids)} lojas e {len(skus)} produtos do banco; "
                  f"entram só pedidos novos")
        else:
            store_ids = list(range(1, args.stores + 1))

            # lojas
            with cur.copy("COPY retail.stores (store_id, store_name, city, state) FROM STDIN") as cp:
                for i in progress(store_ids, "lojas", args.stores):
                    city, st = CITIES[(i - 1) % len(CITIES)]
                    cp.write_row((i, f"Loja {i:03d} {city}", city, st))

            # produtos
            skus = []
            with cur.copy("COPY retail.products (sku, product_name, category, unit_price, perishable) FROM STDIN") as cp:
                for i in progress(range(1, args.products + 1), "produtos", args.products):
                    cat = CATEGORIES[(i - 1) % len(CATEGORIES)]
                    sku = f"SKU{i:05d}"
                    skus.append((sku, round(random.uniform(2.5, 89.9), 2)))
                    cp.write_row((sku, f"{cat} item {i}", cat, skus[-1][1], cat in PERISHABLE))

            # estoque: toda loja x todo produto
            pares = ((s, sku) for s in store_ids for sku, _ in skus)
            with cur.copy("COPY retail.inventory (store_id, sku, on_hand, reorder_pt, updated_at) FROM STDIN") as cp:
                for s, sku in progress(pares, "estoque", len(store_ids) * len(skus)):
                    reorder = random.randint(10, 60)
                    cp.write_row((s, sku, random.randint(0, reorder * 4), reorder, t0))

        # pedidos (primeira carga e append): order_id vem da sequência, então nunca colide.
        # Com o Debezium em streaming, a replicação lógica só entrega a transação depois do commit:
        # os pedidos do append chegam ao Zerobus em rajada, logo depois do commit, e não um a um durante o COPY.
        start = t0 - timedelta(days=args.days)
        span = int(timedelta(days=args.days).total_seconds())
        with cur.copy("COPY retail.orders (store_id, sku, qty, unit_price, status, created_at, updated_at) FROM STDIN") as cp:
            for _ in progress(range(args.orders), "pedidos", args.orders):
                sku, price = random.choice(skus)
                created = start + timedelta(seconds=random.randint(0, span))
                status = random.choice(STATUS_HIST)
                updated = created + timedelta(hours=random.randint(0, 72)) if status != "NEW" else created
                cp.write_row((random.choice(store_ids), sku, random.randint(1, 24), price, status, created, min(updated, t0)))
            # a barra conta linhas ENTREGUES ao COPY. Ao fechar o COPY, o Postgres ainda confere as chaves
            # estrangeiras de cada linha (gatilhos AFTER ROW disparam no fim do comando) e depois vem o commit.
            if not args.sem_fk and args.orders >= 1_000_000:
                print("⏳ linhas enviadas. O Postgres ainda confere loja e produto de cada pedido e faz o commit; "
                      "em cargas grandes isso leva minutos (use --sem-fk na próxima).", flush=True)

        # só a partir daqui as linhas existem para o slot do Debezium
        conn.commit()
        cur.execute("SELECT (SELECT count(*) FROM retail.stores), (SELECT count(*) FROM retail.products), "
                    "(SELECT count(*) FROM retail.inventory), (SELECT count(*) FROM retail.orders)")
        stores, products, inv, orders = cur.fetchone()

    dt = time.perf_counter() - inicio
    if append:
        inseridas = args.orders
        print(f"\n✅ ok em {dt:.1f}s: +{args.orders} pedidos (agora são {orders} na tabela)")
    else:
        inseridas = args.stores + args.products + args.stores * args.products + args.orders
        print(f"\n✅ ok em {dt:.1f}s: {stores} lojas, {products} produtos, {inv} linhas de estoque, {orders} pedidos")
    print(f"⚡ taxa média: {inseridas / dt:.0f} linhas/s")
    return 0


if __name__ == "__main__":
    sys.exit(main())