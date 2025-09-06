CREATE DATABASE olist_db;

USE olist_db;

CREATE TABLE
	olist_orders_dataset (
		order_id VARCHAR(50) NOT NULL,
		customer_id VARCHAR(50) NOT NULL,
		order_status VARCHAR(20) NOT NULL,
		order_purchase_timestamp DATETIME NOT NULL,
		order_approved_at DATETIME NULL,
		order_delivered_carrier_date DATETIME NULL,
		order_delivered_customer_date DATETIME NULL,
		order_estimated_delivery_date DATETIME NOT NULL,
		CONSTRAINT pk_orders PRIMARY KEY (order_id)
	);

CREATE TABLE
	olist_order_payments_dataset (
		order_id VARCHAR(50) NOT NULL,
		payment_sequential INT NOT NULL,
		payment_type VARCHAR(20) NOT NULL,
		payment_installments INT NOT NULL,
		payment_value DECIMAL(10, 2) NOT NULL,
		CONSTRAINT pk_payments PRIMARY KEY (order_id, payment_sequential),
		CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES olist_orders_dataset (order_id)
	);

CREATE INDEX idx_payments_order_id ON olist_order_payments_dataset (order_id);

CREATE TABLE
	olist_order_reviews_dataset (
		review_id VARCHAR(50) NOT NULL,
		order_id VARCHAR(50) NOT NULL,
		review_score TINYINT NOT NULL,
		review_comment_title VARCHAR(255) NULL,
		review_comment_message TEXT NULL,
		review_creation_date DATE NOT NULL,
		review_answer_timestamp DATETIME NULL,
		CONSTRAINT pk_reviews PRIMARY KEY (review_id),
		CONSTRAINT fk_reviews_order FOREIGN KEY (order_id) REFERENCES olist_orders_dataset (order_id)
	);