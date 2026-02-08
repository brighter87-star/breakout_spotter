-- Breakout Spotter 테이블 (asset_us DB에 추가)
-- 접두사 bs_ 로 기존 테이블과 충돌 방지

CREATE TABLE IF NOT EXISTS bs_stocks (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    ticker        VARCHAR(20) NOT NULL,
    name          VARCHAR(200),
    exchange      VARCHAR(10) COMMENT 'NASDAQ, NYSE, AMEX',
    exchange_code VARCHAR(4)  COMMENT 'NAS, NYS, AMS',
    is_active     TINYINT(1) DEFAULT 1,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_ticker (ticker),
    INDEX idx_exchange (exchange_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bs_daily_prices (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    stock_id    INT NOT NULL,
    trade_date  DATE NOT NULL,
    open_price  DECIMAL(12,4) NOT NULL,
    high_price  DECIMAL(12,4) NOT NULL,
    low_price   DECIMAL(12,4) NOT NULL,
    close_price DECIMAL(12,4) NOT NULL,
    volume      BIGINT DEFAULT 0,
    FOREIGN KEY (stock_id) REFERENCES bs_stocks(id),
    UNIQUE KEY uk_stock_date (stock_id, trade_date),
    INDEX idx_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bs_themes (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    name_ko         VARCHAR(100) NOT NULL,
    name_en         VARCHAR(100),
    source_theme_id INT,
    is_active       TINYINT(1) DEFAULT 1,
    UNIQUE KEY uk_source (source_theme_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bs_stock_themes (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    stock_id      INT NOT NULL,
    theme_id      INT NOT NULL,
    report_date   DATE NOT NULL,
    mention_count INT DEFAULT 1,
    FOREIGN KEY (stock_id) REFERENCES bs_stocks(id),
    FOREIGN KEY (theme_id) REFERENCES bs_themes(id),
    UNIQUE KEY uk_mapping (stock_id, theme_id, report_date),
    INDEX idx_theme_date (theme_id, report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bs_breakout_signals (
    id                      INT AUTO_INCREMENT PRIMARY KEY,
    stock_id                INT NOT NULL,
    signal_date             DATE NOT NULL,
    theme_id                INT,
    close_price             DECIMAL(12,4),
    high_60d                DECIMAL(12,4),
    low_lookback            DECIMAL(12,4),
    rise_from_low_pct       DECIMAL(10,4),
    consolidation_days      INT,
    consolidation_range_pct DECIMAL(10,4),
    consolidation_high      DECIMAL(12,4),
    volume_ratio            DECIMAL(10,4),
    theme_relative_str      DECIMAL(10,4),
    signal_score            DECIMAL(5,2) COMMENT '0~100',
    FOREIGN KEY (stock_id) REFERENCES bs_stocks(id),
    UNIQUE KEY uk_signal (stock_id, signal_date),
    INDEX idx_date_score (signal_date, signal_score DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
