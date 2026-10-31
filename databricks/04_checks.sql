-- Zerobus + Debezium Server · Databricks · passo 4 · conferências para a demo e para o artigo
DECLARE OR REPLACE VARIABLE catalog_name STRING DEFAULT '<catalog>';

-- 1) Chegou? Quantos eventos por operação (r = snapshot, c/u/d = streaming)
SELECT __op, count(*) AS eventos, min(__source_ts_ms) AS primeiro_ms, max(__source_ts_ms) AS ultimo_ms
FROM IDENTIFIER(catalog_name || '.retail.orders')
GROUP BY __op ORDER BY __op;

-- 2) Frescor: idade do evento mais recente em segundos (commit no Postgres -> agora)
SELECT round((unix_millis(current_timestamp()) - max(__source_ts_ms)) / 1000.0, 1) AS idade_ultimo_evento_s
FROM IDENTIFIER(catalog_name || '.retail.orders');

-- 3) Atraso Debezium -> commit: diferença entre o Debezium processar e o commit no Postgres
SELECT percentile_approx(__ts_ms - __source_ts_ms, 0.5) AS p50_ms,
       percentile_approx(__ts_ms - __source_ts_ms, 0.95) AS p95_ms,
       max(__ts_ms - __source_ts_ms) AS max_ms
FROM IDENTIFIER(catalog_name || '.retail.orders')
WHERE __op <> 'r';

-- 4) Duplicatas (esperadas ocasionalmente: at-least-once). Mesmo evento = mesmo LSN.
SELECT order_id, __lsn, count(*) AS copias
FROM IDENTIFIER(catalog_name || '.retail.orders')
GROUP BY order_id, __lsn HAVING count(*) > 1
ORDER BY copias DESC LIMIT 20;

-- 5) Bronze x silver: a silver deve ter uma linha por pedido vivo
SELECT (SELECT count(*) FROM IDENTIFIER(catalog_name || '.retail.orders')) AS eventos_bronze,
       (SELECT count(DISTINCT order_id) FROM IDENTIFIER(catalog_name || '.retail.orders')) AS pedidos_distintos_bronze,
       (SELECT count(*) FROM IDENTIFIER(catalog_name || '.retail_silver.orders')) AS pedidos_silver;

-- 6) Um pedido do começo ao fim (troque o id por um que você viu mudar no simulador)
-- SELECT order_id, status, __op, __deleted, __lsn, from_unixtime(__source_ts_ms/1000) AS commit_ts
-- FROM IDENTIFIER(catalog_name || '.retail.orders') WHERE order_id = 123456 ORDER BY __lsn;

-- 7) Tipos chegaram certos? (TIMESTAMP de verdade, DECIMAL de verdade)
DESCRIBE TABLE IDENTIFIER(catalog_name || '.retail.orders');
SELECT order_id, unit_price, created_at, typeof(created_at) AS tipo_created_at
FROM IDENTIFIER(catalog_name || '.retail.orders') LIMIT 3;
