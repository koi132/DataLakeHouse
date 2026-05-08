-- Phan tich danh gia khach hang theo danh muc san pham
SELECT 
    p.product_category_name_english AS category,
    COUNT(*) AS total_reviews,
    ROUND(AVG(f.review_score), 2) AS avg_score,
    SUM(f.is_positive) AS positive_reviews,
    SUM(f.is_negative) AS negative_reviews,
    SUM(f.is_neutral) AS neutral_reviews,
    ROUND(SUM(f.is_positive) * 100.0 / COUNT(*), 2) AS positive_rate_pct
FROM delta.gold.fact_orders f
JOIN delta.gold.dim_product p ON f.product_sk = p.product_sk
JOIN delta.gold.dim_date d ON f.date_sk = d.date_sk
WHERE f.has_review = true
GROUP BY p.product_category_name_english
ORDER BY total_reviews DESC
LIMIT 5
