-- ═══════════════════════════════════════════════════════════
-- KPIs analytiques — à exécuter dans Trino (http://localhost:8080)
-- Sélectionne le catalog "iceberg", schema "gold"
-- ═══════════════════════════════════════════════════════════

-- 1. Nombre d'événements CDC par table et opération
SELECT source_table, operation, SUM(event_count) AS nb_events
FROM iceberg.gold.cdc_audit
GROUP BY source_table, operation
ORDER BY nb_events DESC;

-- 2. Latence CDC (source PostgreSQL → Kafka en secondes)
SELECT source_table, ROUND(AVG(avg_latency_seconds), 2) AS latence_moy_sec
FROM iceberg.gold.cdc_audit
GROUP BY source_table;

-- 3. Évolution des commandes par jour
SELECT order_day, COUNT(*) AS nb_commandes, ROUND(SUM(total_amount),2) AS ca
FROM iceberg.gold.orders
GROUP BY order_day
ORDER BY order_day DESC LIMIT 30;

-- 4. Chiffre d'affaires par catégorie produit
SELECT product_category, ROUND(SUM(line_revenue),2) AS ca,
       COUNT(DISTINCT order_id) AS nb_commandes
FROM iceberg.gold.sales_facts
WHERE is_cancelled = 0
GROUP BY product_category ORDER BY ca DESC;

-- 5. Paiements par méthode et période
SELECT payment_month, payment_year, method,
       SUM(nb_payments) AS nb, ROUND(SUM(total_amount),2) AS montant
FROM iceberg.gold.payments_summary
WHERE status = 'completed'
GROUP BY 1,2,3 ORDER BY payment_year DESC, payment_month DESC;

-- 6. Taux d'annulation
SELECT ROUND(100.0 * SUM(is_cancelled) / COUNT(*), 2) AS taux_annulation_pct
FROM iceberg.gold.orders;

-- 7. Ventes par segment client et canal
SELECT customer_segment, channel,
       COUNT(DISTINCT order_id) AS nb_commandes,
       ROUND(SUM(line_revenue),2) AS ca
FROM iceberg.gold.sales_facts
WHERE is_cancelled = 0
GROUP BY customer_segment, channel ORDER BY ca DESC;

-- 8. Top 10 clients par CA
SELECT customer_id, customer_name, customer_segment,
       COUNT(DISTINCT order_id) AS nb_commandes,
       ROUND(SUM(line_revenue),2) AS ca_total
FROM iceberg.gold.sales_facts
WHERE is_cancelled = 0
GROUP BY 1,2,3 ORDER BY ca_total DESC LIMIT 10;

-- 9. Résumé des retours par raison
SELECT reason, status, SUM(nb_returns) AS nb, ROUND(SUM(total_refund),2) AS remboursé
FROM iceberg.gold.returns_summary
GROUP BY reason, status ORDER BY nb DESC;
