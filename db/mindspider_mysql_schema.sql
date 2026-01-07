-- MindSpider 社媒数据落库（MySQL）
-- 不创建 database；直接在 config.ini 的 [mysql].database 下创建表

CREATE TABLE IF NOT EXISTS mindspider_post (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  stock_code VARCHAR(16) NOT NULL,
  platform VARCHAR(32) NOT NULL,           -- wb / xhs / ...
  keyword VARCHAR(128) NULL,               -- 触发采集的关键词
  platform_post_id VARCHAR(128) NULL,      -- 平台侧 id（若可获得）
  url VARCHAR(1024) NULL,
  title VARCHAR(512) NULL,
  author VARCHAR(128) NULL,
  published_at DATETIME NULL,              -- 平台发布时间（若可解析）
  content LONGTEXT NULL,                   -- 正文/文本
  like_count INT NULL,
  comment_count INT NULL,
  share_count INT NULL,
  collect_count INT NULL,
  raw_json JSON NULL,                      -- 原始结构化返回（便于追溯/补字段）
  crawled_at DATETIME NOT NULL,
  source VARCHAR(32) NOT NULL DEFAULT 'mindspider',
  PRIMARY KEY (id),
  UNIQUE KEY uk_platform_pid (platform, platform_post_id),
  KEY idx_stock_pub (stock_code, published_at),
  KEY idx_stock_crawl (stock_code, crawled_at),
  KEY idx_platform (platform)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
