-- Zerobus + Debezium Server · consultas do vídeo (bloco 7 e conferências da carga)
-- Para rodar no VS Code com SQLTools + Databricks Driver: selecione UMA consulta,
-- botão direito > Run Selected Query. O resultado abre numa aba nova.

-- ===================== Carga de 10 GB (segunda) =====================

-- durante a carga: a contagem subindo
SELECT count(*) AS linhas_snapshot
FROM retail_zerobus.retail.orders
WHERE __op = 'r';

-- depois da carga: as duas colunas têm de bater com o seed
SELECT count(*) AS linhas, count(DISTINCT order_id) AS pedidos
FROM retail_zerobus.retail.orders
WHERE __op = 'r';

-- tamanho e número de arquivos da bronze (sizeInBytes, numFiles)
DESCRIBE DETAIL retail_zerobus.retail.orders;

-- ===================== Demo ao vivo (bloco 7) =====================

-- A. eventos por operação (r = snapshot, c/u/d = CDC)
SELECT __op, count(*) AS eventos
FROM retail_zerobus.retail.orders
GROUP BY __op ORDER BY __op;

-- B. os últimos eventos chegando
SELECT order_id, status, __op, __deleted,
       timestamp_millis(__source_ts_ms) AS commit_no_postgres
FROM retail_zerobus.retail.orders
WHERE __op <> 'r'
ORDER BY __lsn DESC
LIMIT 20;

-- C. há quantos segundos foi o último commit que já está na bronze
SELECT round((unix_millis(current_timestamp()) - max(__source_ts_ms)) / 1000.0, 1) AS segundos_atras
FROM retail_zerobus.retail.orders
WHERE __op <> 'r';

-- D. um pedido do começo ao fim (troque o id por um da consulta B)
SELECT order_id, status, __op, __lsn, timestamp_millis(__source_ts_ms) AS commit_no_postgres
FROM retail_zerobus.retail.orders
WHERE order_id = 94205900
ORDER BY __lsn;

-- E. pedidos vivos: o último evento de cada pedido, sem os deletados
--    (compare com SELECT count(*) FROM retail.orders; no Postgres, com o simulador parado)
SELECT count(*) AS pedidos_vivos
FROM (SELECT order_id, max_by(__deleted, __lsn) AS deletado
      FROM retail_zerobus.retail.orders
      GROUP BY order_id)
WHERE deletado = 'false';

-- F. duplicatas
SELECT count(*) - count(DISTINCT order_id, __lsn) AS duplicados
FROM retail_zerobus.retail.orders;

-- G. só a captura, do commit até o Debezium (p50)
SELECT percentile(__ts_ms - __source_ts_ms, 0.5) AS captura_p50_ms
FROM retail_zerobus.retail.orders
WHERE __op IN ('c', 'u', 'd');
