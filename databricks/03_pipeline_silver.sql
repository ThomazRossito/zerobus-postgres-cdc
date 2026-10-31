-- Zerobus + Debezium Server · Databricks · passo 3 · pipeline Lakeflow Spark Declarative Pipelines
-- Arquivo-fonte do pipeline (crie um pipeline serverless, catálogo = seu catálogo, schema padrão = retail_silver,
-- e aponte este arquivo como source). A bronze é append-only; a silver é o estado atual (SCD tipo 1).
--
-- Troque <catalog> pelo seu catálogo. Em pipelines SDP não há DECLARE VARIABLE; use parâmetros do pipeline
-- (configuration: catalog=...) e ${catalog} se preferir.

-- Estado atual dos pedidos
CREATE OR REFRESH STREAMING TABLE orders
  COMMENT 'Estado atual de retail.orders, materializado a partir dos eventos CDC da bronze';

CREATE FLOW orders_cdc AS AUTO CDC INTO orders
FROM STREAM(<catalog>.retail.orders)
KEYS (order_id)
APPLY AS DELETE WHEN __deleted = 'true'
SEQUENCE BY __lsn
COLUMNS * EXCEPT (__op, __ts_ms, __source_ts_ms, __lsn, __deleted)
STORED AS SCD TYPE 1;

-- Estado atual do estoque
CREATE OR REFRESH STREAMING TABLE inventory
  COMMENT 'Estado atual de retail.inventory, materializado a partir dos eventos CDC da bronze';

CREATE FLOW inventory_cdc AS AUTO CDC INTO inventory
FROM STREAM(<catalog>.retail.inventory)
KEYS (store_id, sku)
APPLY AS DELETE WHEN __deleted = 'true'
SEQUENCE BY __lsn
COLUMNS * EXCEPT (__op, __ts_ms, __source_ts_ms, __lsn, __deleted)
STORED AS SCD TYPE 1;

-- Opcional: histórico completo dos pedidos (SCD tipo 2), útil para mostrar a diferença na demo
-- CREATE OR REFRESH STREAMING TABLE orders_history;
-- CREATE FLOW orders_hist AS AUTO CDC INTO orders_history
-- FROM STREAM(<catalog>.retail.orders)
-- KEYS (order_id)
-- APPLY AS DELETE WHEN __deleted = 'true'
-- SEQUENCE BY __lsn
-- COLUMNS * EXCEPT (__op, __ts_ms, __source_ts_ms, __lsn, __deleted)
-- STORED AS SCD TYPE 2;

-- Observações
-- * SEQUENCE BY __lsn, e não updated_at: o LSN é a ordem real do WAL e resolve duplicatas do at-least-once
--   (o mesmo evento reenviado tem o mesmo LSN e não "avança" o estado).
-- * Eventos de snapshot (__op = 'r') entram como upsert normal, então a silver nasce completa.
