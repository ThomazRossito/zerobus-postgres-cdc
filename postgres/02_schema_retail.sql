-- Zerobus + Debezium Server · passo 2 · schema de varejo no banco da demo
-- Rode no banco escolhido (ex.: psql -U postgres -d retail_demo -f postgres/02_schema_retail.sql)
-- Crie o banco antes se não existir:  CREATE DATABASE retail_demo;
--
-- Decisões que viram colunas no Databricks (modo "typed" do sink Zerobus):
--   * timestamp SEM fuso (UTC) vira TIMESTAMP no Delta.
--     timestamptz viraria STRING ISO-8601 (mapa oficial do sink). Por isso usamos timestamp + UTC.
--   * numeric(10,2) vira DECIMAL(10,2) (decimal.handling.mode=precise, padrão do Debezium).
--   * REPLICA IDENTITY FULL: update e delete chegam com a linha inteira em "before",
--     o que deixa o delete reescrito (__deleted='true') com todas as colunas preenchidas.

CREATE SCHEMA IF NOT EXISTS retail;

CREATE TABLE IF NOT EXISTS retail.stores (
  store_id    integer PRIMARY KEY,
  store_name  text NOT NULL,
  city        text NOT NULL,
  state       char(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS retail.products (
  sku           text PRIMARY KEY,
  product_name  text NOT NULL,
  category      text NOT NULL,
  unit_price    numeric(10,2) NOT NULL,
  perishable    boolean NOT NULL DEFAULT false
);

-- Capturada pelo Debezium
CREATE TABLE IF NOT EXISTS retail.inventory (
  store_id    integer NOT NULL REFERENCES retail.stores(store_id),
  sku         text    NOT NULL REFERENCES retail.products(sku),
  on_hand     integer NOT NULL,
  reorder_pt  integer NOT NULL,
  updated_at  timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
  PRIMARY KEY (store_id, sku)
);

-- Capturada pelo Debezium
CREATE TABLE IF NOT EXISTS retail.orders (
  order_id    bigserial PRIMARY KEY,
  store_id    integer NOT NULL REFERENCES retail.stores(store_id),
  sku         text    NOT NULL REFERENCES retail.products(sku),
  qty         integer NOT NULL,
  unit_price  numeric(10,2) NOT NULL,
  status      text NOT NULL,            -- NEW, PICKING, SHIPPED, DELIVERED, CANCELLED
  created_at  timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
  updated_at  timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

ALTER TABLE retail.orders    REPLICA IDENTITY FULL;
ALTER TABLE retail.inventory REPLICA IDENTITY FULL;

-- Permissões para o usuário de replicação (criado em 01_replication.sql)
GRANT USAGE ON SCHEMA retail TO dbz;
GRANT SELECT ON ALL TABLES IN SCHEMA retail TO dbz;
ALTER DEFAULT PRIVILEGES IN SCHEMA retail GRANT SELECT ON TABLES TO dbz;

-- Publicação só com as tabelas capturadas (publication.autocreate.mode=disabled no Debezium)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'zerobus_pub') THEN
    CREATE PUBLICATION zerobus_pub FOR TABLE retail.orders, retail.inventory;
  END IF;
END $$;

-- Conferências
-- SELECT * FROM pg_publication_tables WHERE pubname = 'zerobus_pub';
-- SELECT relname, relreplident FROM pg_class WHERE relname IN ('orders','inventory');  -- 'f' = FULL
