-- Demo Zerobus · catálogo retail_zerobus
-- Passo 3a: schemas e tabelas bronze. Rode tudo no SQL Editor, com um SQL warehouse.
-- O Zerobus nunca cria nem altera tabela: elas precisam existir antes do Debezium subir.
-- Colunas = as do Postgres + os metadados do unwrap (__op, __ts_ms, __source_ts_ms, __lsn, __deleted).

CREATE SCHEMA IF NOT EXISTS retail_zerobus.retail
  COMMENT 'Bronze: eventos CDC do PostgreSQL local via Debezium Server + Zerobus Ingest';

CREATE SCHEMA IF NOT EXISTS retail_zerobus.retail_silver
  COMMENT 'Silver: estado atual via AUTO CDC no Lakeflow SDP';

CREATE TABLE IF NOT EXISTS retail_zerobus.retail.orders (
  order_id        BIGINT        COMMENT 'PK no Postgres',
  store_id        INT,
  sku             STRING,
  qty             INT,
  unit_price      DECIMAL(10,2),
  status          STRING        COMMENT 'NEW, PICKING, SHIPPED, DELIVERED, CANCELLED',
  created_at      TIMESTAMP,
  updated_at      TIMESTAMP,
  __op            STRING        COMMENT 'Debezium: c=create, u=update, d=delete, r=snapshot read',
  __ts_ms         BIGINT        COMMENT 'epoch ms em que o Debezium processou o evento',
  __source_ts_ms  BIGINT        COMMENT 'epoch ms do commit no Postgres',
  __lsn           BIGINT        COMMENT 'LSN do WAL: ordem dos eventos (SEQUENCE BY na silver)',
  __deleted       STRING        COMMENT 'true quando o evento é um delete reescrito pelo unwrap'
)
COMMENT 'Bronze CDC de retail.orders (append-only, alimentado pelo Zerobus)';

CREATE TABLE IF NOT EXISTS retail_zerobus.retail.inventory (
  store_id        INT,
  sku             STRING,
  on_hand         INT,
  reorder_pt      INT,
  updated_at      TIMESTAMP,
  __op            STRING,
  __ts_ms         BIGINT,
  __source_ts_ms  BIGINT,
  __lsn           BIGINT,
  __deleted       STRING
)
COMMENT 'Bronze CDC de retail.inventory (append-only, alimentado pelo Zerobus)';

-- Conferência: as duas tabelas existem...
SHOW TABLES IN retail_zerobus.retail;
-- ...e não têm Change Data Feed nem column mapping (o Zerobus exige os dois desligados).
-- delta.enableChangeDataFeed e delta.columnMapping.mode não devem aparecer, ou devem estar false / none.
SHOW TBLPROPERTIES retail_zerobus.retail.orders;