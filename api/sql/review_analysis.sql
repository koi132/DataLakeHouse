-- Phân tích đánh giá khách hàng theo danh mục sản phẩm
SELECT 
    p.product_category_name_english AS category,
    COUNT(*) AS total_reviews,
    ROUND(AVG(r.review_score), 2) AS avg_score,
    SUM(CASE WHEN r.is_positive = 1 THEN 1 ELSE 0 END) AS positive_reviews,
    SUM(CASE WHEN r.is_negative = 1 THEN 1 ELSE 0 END) AS negative_reviews,
    SUM(CASE WHEN r.is_neutral = 1 THEN 1 ELSE 0 END) AS neutral_reviews,
    ROUND(SUM(CASE WHEN r.is_positive = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS positive_rate_pct
FROM delta.gold.fact_reviews r
JOIN delta.gold.dim_product p ON r.product_sk = p.product_sk
JOIN delta.gold.dim_date d ON r.review_date_sk = d.date_sk
GROUP BY p.product_category_name_english
ORDER BY total_reviews DESC
LIMIT 5
