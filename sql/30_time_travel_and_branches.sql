-- ============================================================================
-- TIME-TRAVEL (Iceberg) + BRANCHES (Nessie)
-- ============================================================================

-- 1) Histórico de snapshots da tabela (cada write gera um snapshot imutável)
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg.sales."orders$snapshots"
ORDER BY committed_at DESC
LIMIT 10;

-- 2) Time-travel: ler a tabela em um snapshot específico
--    (troque <SNAPSHOT_ID> por um id da query acima)
-- SELECT count(*) FROM iceberg.sales.orders FOR VERSION AS OF <SNAPSHOT_ID>;

-- 3) Time-travel por timestamp
-- SELECT count(*) FROM iceberg.sales.orders
-- FOR TIMESTAMP AS OF TIMESTAMP '2024-09-01 00:00:00 UTC';

-- ----------------------------------------------------------------------------
-- BRANCHES (data-as-code) são gerenciados pelo Nessie, não por SQL no Trino.
-- O catálogo `iceberg`     aponta para o branch `main`.
-- O catálogo `iceberg_dev` aponta para o branch `etl_branch`.
--
-- Depois de rodar a demo (job data_as_code), você pode comparar os dois refs:
--   SELECT count(*) FROM iceberg.sales.orders;        -- main
--   SELECT count(*) FROM iceberg_dev.sales.orders;    -- etl_branch (se existir)
--
-- Gerência de branches via API Nessie (exemplos com curl):
--   Listar refs:  curl -s http://localhost:19120/api/v2/trees | jq
--   Ver main:     curl -s http://localhost:19120/api/v2/trees/main | jq
-- ----------------------------------------------------------------------------
