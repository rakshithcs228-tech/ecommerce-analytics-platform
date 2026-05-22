-- ╔══════════════════════════════════════════════════════════════╗
-- ║  E-COMMERCE ANALYTICS — SAMPLE BUSINESS QUERIES             ║
-- ║  Run these in Hive to demonstrate SQL analytics             ║
-- ╚══════════════════════════════════════════════════════════════╝

USE ecommerce;

-- ─── QUERY 1: Total revenue by region ─────────────────────────
-- Business question: Which region generates most revenue?
SELECT
    region,
    COUNT(*)                        AS total_orders,
    ROUND(SUM(final_amount), 2)     AS total_revenue,
    ROUND(AVG(final_amount), 2)     AS avg_order_value,
    SUM(CASE WHEN is_high_value = true THEN 1 ELSE 0 END)
                                    AS high_value_orders
FROM orders
GROUP BY region
ORDER BY total_revenue DESC;


-- ─── QUERY 2: Peak ordering hours ─────────────────────────────
-- Business question: When do most orders arrive?
SELECT
    order_hour,
    COUNT(*)                        AS total_orders,
    ROUND(SUM(final_amount), 2)     AS total_revenue,
    ROUND(AVG(final_amount), 2)     AS avg_order_value
FROM orders
GROUP BY order_hour
ORDER BY order_hour ASC;


-- ─── QUERY 3: Device breakdown ─────────────────────────────────
-- Business question: Mobile vs Desktop vs Tablet?
SELECT
    device,
    COUNT(*)                        AS total_orders,
    ROUND(SUM(final_amount), 2)     AS total_revenue,
    ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER(), 2)    AS percentage
FROM orders
GROUP BY device
ORDER BY total_orders DESC;


-- ─── QUERY 4: Value segment distribution ──────────────────────
-- Business question: How many premium vs low value customers?
SELECT
    value_segment,
    COUNT(*)                        AS total_orders,
    ROUND(SUM(final_amount), 2)     AS total_revenue,
    ROUND(AVG(final_amount), 2)     AS avg_order_value
FROM orders
GROUP BY value_segment
ORDER BY
    CASE value_segment
        WHEN 'PREMIUM' THEN 1
        WHEN 'HIGH'    THEN 2
        WHEN 'MEDIUM'  THEN 3
        WHEN 'LOW'     THEN 4
    END;


-- ─── QUERY 5: Payment method success rates ────────────────────
-- Business question: Which payment method fails most?
SELECT
    payment_method,
    COUNT(*)                        AS total_transactions,
    SUM(CASE WHEN is_successful = true  THEN 1 ELSE 0 END)
                                    AS successful,
    SUM(CASE WHEN is_successful = false THEN 1 ELSE 0 END)
                                    AS failed,
    ROUND(
        SUM(CASE WHEN is_successful = true THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2)      AS success_rate_pct,
    ROUND(SUM(amount), 2)           AS total_amount
FROM payments
GROUP BY payment_method
ORDER BY total_transactions DESC;


-- ─── QUERY 6: Revenue trend by day ────────────────────────────
-- Business question: How is revenue growing day over day?
SELECT
    date,
    COUNT(*)                        AS total_orders,
    ROUND(SUM(final_amount), 2)     AS daily_revenue,
    ROUND(AVG(final_amount), 2)     AS avg_order_value,
    SUM(CASE WHEN discount > 0 THEN 1 ELSE 0 END)
                                    AS discounted_orders
FROM orders
GROUP BY date
ORDER BY date ASC;


-- ─── QUERY 7: User event funnel ───────────────────────────────
-- Business question: Where do users drop off?
SELECT
    event_type,
    COUNT(*)                        AS total_events,
    COUNT(DISTINCT customer_id)     AS unique_customers,
    ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER(), 2)    AS event_share_pct
FROM user_events
GROUP BY event_type
ORDER BY total_events DESC;


-- ─── QUERY 8: Top cities by revenue ───────────────────────────
SELECT
    city,
    region,
    COUNT(*)                        AS total_orders,
    ROUND(SUM(final_amount), 2)     AS total_revenue
FROM orders
GROUP BY city, region
ORDER BY total_revenue DESC
LIMIT 10;


-- ─── QUERY 9: Weekend vs Weekday orders ───────────────────────
-- 1=Sunday, 2=Monday ... 6=Friday, 7=Saturday
SELECT
    CASE
        WHEN order_day_of_week IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END                             AS day_type,
    COUNT(*)                        AS total_orders,
    ROUND(SUM(final_amount), 2)     AS total_revenue,
    ROUND(AVG(final_amount), 2)     AS avg_order_value
FROM orders
GROUP BY
    CASE
        WHEN order_day_of_week IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END;


-- ─── QUERY 10: Customer RFM segments (after Airflow runs) ─────
SELECT
    rfm_segment,
    COUNT(*)                        AS customer_count,
    ROUND(AVG(total_revenue), 2)    AS avg_lifetime_value,
    ROUND(AVG(total_orders), 2)     AS avg_orders_per_customer
FROM customer_rfm
GROUP BY rfm_segment
ORDER BY avg_lifetime_value DESC;