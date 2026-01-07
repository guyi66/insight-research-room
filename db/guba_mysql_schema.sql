-- MySQL schema for EastMoney Guba backup database
-- charset: utf8mb4 (recommended)

CREATE DATABASE IF NOT EXISTS `bettafish_guba`
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE `bettafish_guba`;

CREATE TABLE IF NOT EXISTS guba_post (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  stock_code VARCHAR(16) NOT NULL,
  href VARCHAR(512) NOT NULL,
  url VARCHAR(768) NULL,
  title VARCHAR(512) NULL,
  author VARCHAR(128) NULL,
  read_count INT NULL,
  comment_count INT NULL,
  last_update_raw VARCHAR(64) NULL,
  last_update_dt DATETIME NULL,
  crawled_at DATETIME NOT NULL,
  full_text LONGTEXT NULL,
  full_text_time_raw VARCHAR(64) NULL,
  full_text_dt DATETIME NULL,
  source VARCHAR(32) NOT NULL DEFAULT 'eastmoney_guba',
  PRIMARY KEY (id),
  UNIQUE KEY uk_href (href),
  KEY idx_stock_time (stock_code, last_update_dt),
  KEY idx_stock_crawled (stock_code, crawled_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
