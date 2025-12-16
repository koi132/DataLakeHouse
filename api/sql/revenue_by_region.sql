-- Doanh thu theo vùng địa lý
SELECT 
    g.region,
    g.state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_item_value) AS total_revenue,
    AVG(f.total_item_value) AS avg_order_value,
    SUM(f.freight_value) AS total_shipping_cost
FROM delta.gold.fact_order_items f
JOIN delta.gold.dim_geography g ON f.customer_geography_sk = g.geography_sk
JOIN delta.gold.dim_date d ON f.date_sk = d.date_sk
GROUP BY g.region, g.state
ORDER BY total_revenue DESC
LIMIT 5
