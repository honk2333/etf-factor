CREATE TABLE IF NOT EXISTS factor_definitions (
    factor_key VARCHAR PRIMARY KEY,
    factor_name VARCHAR NOT NULL,
    params_json VARCHAR NOT NULL,
    source_table VARCHAR NOT NULL DEFAULT 'etf_daily',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS factor_values (
    trade_date DATE NOT NULL,
    symbol VARCHAR NOT NULL,
    factor_key VARCHAR NOT NULL,
    factor_name VARCHAR NOT NULL,
    value DOUBLE,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, symbol, factor_key)
);

CREATE TABLE IF NOT EXISTS factor_ic_daily (
    trade_date DATE NOT NULL,
    factor_key VARCHAR NOT NULL,
    factor_name VARCHAR NOT NULL,
    pred_days INTEGER NOT NULL,
    rank_ic DOUBLE,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, factor_key, pred_days)
);

CREATE TABLE IF NOT EXISTS factor_metrics (
    factor_key VARCHAR NOT NULL,
    factor_name VARCHAR NOT NULL,
    pred_days INTEGER NOT NULL,
    ic_mean DOUBLE,
    ic_std DOUBLE,
    ic_ir DOUBLE,
    observation_count BIGINT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (factor_key, pred_days)
);

CREATE INDEX IF NOT EXISTS idx_factor_values_factor_date
ON factor_values (factor_key, trade_date);

CREATE INDEX IF NOT EXISTS idx_factor_values_symbol_date
ON factor_values (symbol, trade_date);
