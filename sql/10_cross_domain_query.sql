-- ============================================================================
-- ZERO-COPY CROSS-DOMAIN QUERY
-- Junta dois domínios (sales x marketing) SEM copiar nenhum dado.
-- Rode no Trino:  docker compose exec trino trino -f /dev/stdin < sql/10_cross_domain_query.sql
-- ou abra a CLI:  docker compose exec -it trino trino
-- ============================================================================

-- Quais catálogos/schemas existem (cada schema = um domínio)
SHOW SCHEMAS FROM iceberg;

-- Receita e ROI por campanha: JOIN entre os domínios, zero duplicação.
SELECT
    c.campaign_name,
    c.channel,
    c.budget,
    count(o.order_id)               AS orders_count,
    sum(o.amount)                   AS revenue,
    round(sum(o.amount) / c.budget, 2) AS roi
FROM iceberg.marketing.campaigns c
JOIN iceberg.sales.orders o
      ON o.campaign_id = c.campaign_id
GROUP BY c.campaign_name, c.channel, c.budget
ORDER BY revenue DESC;

-- Receita por canal e país — cruzando os dois domínios de novo.
SELECT
    c.channel,
    o.country,
    sum(o.amount) AS revenue
FROM iceberg.sales.orders o
JOIN iceberg.marketing.campaigns c ON c.campaign_id = o.campaign_id
GROUP BY c.channel, o.country
ORDER BY revenue DESC
LIMIT 20;
