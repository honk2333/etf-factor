CREATE TABLE IF NOT EXISTS etf_factor (
    trade_date DATE NOT NULL,
    symbol VARCHAR NOT NULL,
    factor_key VARCHAR NOT NULL,
    factor_name VARCHAR NOT NULL,
    params_json VARCHAR NOT NULL,
    value DOUBLE,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, symbol, factor_key)
);

CREATE INDEX IF NOT EXISTS idx_etf_factor_factor_date
ON etf_factor (factor_key, trade_date);

CREATE INDEX IF NOT EXISTS idx_etf_factor_symbol_date
ON etf_factor (symbol, trade_date);
