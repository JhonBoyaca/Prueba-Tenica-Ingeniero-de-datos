USE olist_db;

-- Distribucion de ordes por estado
SELECT order_status, COUNT(*) AS status_count FROM olist_orders_dataset GROUP BY order_status;

-- TOP 3 de metodos de pagos mas utilizados
SELECT payment_type, COUNT(*) AS payment_count FROM olist_order_payments_dataset GROUP BY payment_type ORDER BY payment_count DESC LIMIT 3;

-- TOP 3 de puntuaciones de reseñas de clientes
SELECT review_score, COUNT(*) AS orders_count FROM olist_order_reviews_dataset GROUP BY review_score ORDER BY orders_count DESC LIMIT 3;

-- Ordenes con mayor cantidad de pagos
SELECT order_id, COUNT(*) AS payment_count FROM olist_order_payments_dataset GROUP BY order_id ORDER BY payment_count DESC LIMIT 3;

-- Numero de comentarios con mensajes vacios
SELECT COUNT(*) AS empty_reviews_count FROM olist_order_reviews_dataset WHERE review_comment_message IS NULL;

-- Promedio de las puntuaciones de las reseñas
SELECT AVG(review_score) AS average_review_score FROM olist_order_reviews_dataset;

