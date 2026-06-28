-- ============================================================================
-- HIDDEN PARTITIONING + COMPACTION/SORT (otimização de custo/performance)
-- A tabela orders é particionada por day(order_ts) — partição "escondida":
-- você filtra por order_ts (timestamp) e o Iceberg faz partition pruning sozinho,
-- sem coluna extra de partição e sem o usuário saber do layout físico.
-- ============================================================================

-- 1) Veja as partições materializadas (metadados do Iceberg)
SELECT partition, record_count, file_count, total_size
FROM iceberg.sales."orders$partitions"
ORDER BY 1
LIMIT 20;

-- 2) Partition pruning: o EXPLAIN mostra que só as partições do range são lidas.
EXPLAIN
SELECT count(*), sum(amount)
FROM iceberg.sales.orders
WHERE order_ts >= TIMESTAMP '2024-06-01 00:00:00'
  AND order_ts <  TIMESTAMP '2024-07-01 00:00:00';

-- 3) Clustering por sort (locality multi-dimensional, "estilo Z-order").
--    Reduz arquivos lidos em filtros por essas colunas.
ALTER TABLE iceberg.sales.orders
    SET PROPERTIES sorted_by = ARRAY['country', 'campaign_id'];

-- 4) Compaction: junta arquivos pequenos em arquivos maiores e ordenados.
--    Menos arquivos => menos I/O => menos custo de scan.
ALTER TABLE iceberg.sales.orders EXECUTE optimize;

-- 5) Confira o tamanho/contagem de arquivos depois da compaction
SELECT count(*) AS files, sum(file_size_in_bytes) AS bytes
FROM iceberg.sales."orders$files";

-- ----------------------------------------------------------------------------
-- NOTA sobre Z-Order: o Trino faz compaction + sorted_by (ordenação linear).
-- O Z-Order verdadeiro (curva de Hilbert/Morton) do Iceberg é aplicado via Spark:
--   CALL system.rewrite_data_files(
--     table => 'sales.orders',
--     strategy => 'sort',
--     sort_order => 'zorder(country, campaign_id)'
--   );
-- O efeito de negócio (menos arquivos lidos em filtros multi-coluna) é o mesmo.
-- ----------------------------------------------------------------------------
