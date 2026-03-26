from __future__ import annotations

import json
from hashlib import md5

import numpy as np
import pandas as pd


def _factor_key(factor_name: str, params: dict) -> str:
    payload = json.dumps(params, sort_keys=True, ensure_ascii=False)
    digest = md5(f"{factor_name}|{payload}".encode("utf-8"), usedforsecurity=False).hexdigest()[:12]
    return f"{factor_name}_{digest}"


def factor_identity(factor_func, params: dict) -> tuple[str, str, str]:
    factor_name = factor_func.__name__
    params_json = json.dumps(params, sort_keys=True, ensure_ascii=False)
    factor_key = _factor_key(factor_name, params)
    return factor_key, factor_name, params_json


# --- price ---
def close_ma(df: pd.DataFrame, window: int = 20) -> pd.Series:
    ma = df["close"].rolling(window=window).mean()
    return (df["close"] - ma) / ma


def momentum(df: pd.DataFrame, window: int = 20) -> pd.Series:
    return df["close"].pct_change(periods=window)


def short_reversal(df: pd.DataFrame, window: int = 5) -> pd.Series:
    return -df["close"].pct_change(periods=window)


def close_open_n(df: pd.DataFrame, window: int = 5) -> pd.Series:
    return df["close"] / df["open"].shift(window) - 1


def ma_spread(df: pd.DataFrame, short: int = 5, long: int = 20) -> pd.Series:
    ma_short = df["close"].rolling(window=short).mean()
    ma_long = df["close"].rolling(window=long).mean()
    return (ma_short - ma_long) / ma_long


def breakout(df: pd.DataFrame, window: int = 20) -> pd.Series:
    rolling_high = df["high"].shift(1).rolling(window=window).max()
    return df["close"] - rolling_high


def price_position(df: pd.DataFrame, window: int = 20) -> pd.Series:
    rolling_low = df["low"].rolling(window=window).min()
    rolling_high = df["high"].rolling(window=window).max()
    return (df["close"] - rolling_low) / (rolling_high - rolling_low + 1e-9) - 0.5


def momentum_vol_adj(df: pd.DataFrame, window: int = 20) -> pd.Series:
    mom = df["close"].pct_change(periods=window)
    vol = df["close"].pct_change().rolling(window=window).std()
    return mom / (vol + 1e-9)


def efficiency_ratio(df: pd.DataFrame, window: int = 20) -> pd.Series:
    direction = (df["close"] - df["close"].shift(window)).abs()
    volatility_path = df["close"].diff().abs().rolling(window=window).sum()
    return direction / (volatility_path + 1e-9)


# --- volatility ---
def volatility(df: pd.DataFrame, window: int = 20) -> pd.Series:
    return df["pct_chg"].rolling(window=window).std()


def skewness(df: pd.DataFrame, window: int = 20) -> pd.Series:
    return df["pct_chg"].rolling(window=window).skew()


def kurtosis(df: pd.DataFrame, window: int = 20) -> pd.Series:
    return df["pct_chg"].rolling(window=window).kurt()


def amplitude_factor(df: pd.DataFrame, window: int = 20) -> pd.Series:
    return df["amplitude"].rolling(window=window).mean()


def max_drawdown(df: pd.DataFrame, window: int = 20) -> pd.Series:
    roll_max = df["close"].rolling(window, min_periods=1).max()
    drawdown = (df["close"] - roll_max) / roll_max
    return drawdown.rolling(window).min()


def atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3
    high_low = df["high"] - df["low"]
    high_prev = (df["high"] - df["close"].shift(1)).abs()
    low_prev = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_prev, low_prev], axis=1).max(axis=1)
    tr_rel = tr / typical
    return tr_rel.rolling(window, min_periods=1).mean()


def boll(df: pd.DataFrame, window: int = 20) -> pd.Series:
    mid = df["close"].rolling(window).mean()
    std = df["close"].rolling(window).std()
    return std / mid


def downside_volatility(df: pd.DataFrame, window: int = 20) -> pd.Series:
    downside = df["close"].pct_change().clip(upper=0)
    return downside.rolling(window=window).std()


def upside_downside_ratio(df: pd.DataFrame, window: int = 20) -> pd.Series:
    ret = df["close"].pct_change()
    upside = ret.clip(lower=0).rolling(window=window).std()
    downside = ret.clip(upper=0).abs().rolling(window=window).std()
    return upside / (downside + 1e-9)


# --- price-volume ---
def volume_pct(df: pd.DataFrame, window: int = 20) -> pd.Series:
    return df["volume"].rolling(window).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1])


def volume_ratio(df: pd.DataFrame, window: int = 20) -> pd.Series:
    return df["volume"] / df["volume"].rolling(window).mean()


def obv(df: pd.DataFrame, window: int = 20) -> pd.Series:
    direction = df["close"].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else np.nan))
    signed_vol = direction * df["volume"]
    return signed_vol.rolling(window=window, min_periods=1).sum()


def obv_amount(df: pd.DataFrame, window: int = 20) -> pd.Series:
    direction = df["close"].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else np.nan))
    signed_amt = direction * df["amount"]
    return signed_amt.rolling(window=window, min_periods=1).sum()


def pvt(df: pd.DataFrame, window: int = 20) -> pd.Series:
    pvt_series = df["pct_chg"] * df["amount"]
    return pvt_series.rolling(window=window, min_periods=1).sum()


def amp_vol(df: pd.DataFrame, window: int = 20) -> pd.Series:
    return (df["amplitude"] * df["amount"]).rolling(window).mean()


def amount_amplitude_ratio(df: pd.DataFrame, window: int = 20) -> pd.Series:
    return (df["amount"] / (df["amplitude"] + 1e-9)).rolling(window=window).mean()


def amihud_illiquidity(df: pd.DataFrame, window: int = 20) -> pd.Series:
    ret = df["close"].pct_change().abs()
    return (ret / (df["amount"] + 1.0)).rolling(window=window).mean()


def chaikin_money_flow(df: pd.DataFrame, window: int = 20) -> pd.Series:
    mfm = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / (df["high"] - df["low"] + 1e-9)
    mfv = mfm * df["amount"]
    return mfv.rolling(window=window).sum() / (df["amount"].rolling(window=window).sum() + 1e-9)


def price_volume_corr(df: pd.DataFrame, window: int = 20) -> pd.Series:
    ret = df["close"].pct_change()
    amt_chg = df["amount"].pct_change()
    return ret.rolling(window=window).corr(amt_chg)


# --- indicators ---
def rsi(df: pd.DataFrame, window: int = 14) -> pd.Series:
    delta = df["close"].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.rolling(window).mean()
    ma_down = down.rolling(window).mean()
    rs = ma_up / (ma_down + 1e-9)
    return 100 - 100 / (1 + rs)


def macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
    price = df["close"]
    ema_fast = price.ewm(span=fast, adjust=False).mean()
    ema_slow = price.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line - signal_line


FACTOR_REGISTRY = [
    (close_ma, [{"window": w} for w in [3, 5, 10, 15, 20, 40, 60, 120]]),
    (momentum, [{"window": w} for w in [1, 3, 5, 10, 15, 20, 40, 60, 120]]),
    (short_reversal, [{"window": w} for w in [1, 3, 5, 10]]),
    (close_open_n, [{"window": w} for w in [1, 3, 5]]),
    (ma_spread, [{"short": s, "long": l} for s, l in [(3, 15), (3, 20), (5, 20), (10, 20), (10, 40), (10, 60), (20, 80), (30, 120)]]),
    (breakout, [{"window": w} for w in [3, 5, 10, 20, 40, 60]]),
    (price_position, [{"window": w} for w in [5, 10, 20, 40, 60, 120]]),
    (momentum_vol_adj, [{"window": w} for w in [5, 10, 20, 40, 60, 120]]),
    (efficiency_ratio, [{"window": w} for w in [5, 10, 20, 40, 60, 120]]),
    (volatility, [{"window": w} for w in [3, 5, 10, 15, 20, 40, 60, 120]]),
    (skewness, [{"window": w} for w in [3, 5, 10, 15, 20, 40, 60, 120]]),
    (kurtosis, [{"window": w} for w in [5, 10, 15, 20, 40, 60, 120]]),
    (amplitude_factor, [{"window": w} for w in [1, 3, 5, 10, 15, 20, 40, 60, 120]]),
    (max_drawdown, [{"window": w} for w in [5, 10, 15, 20, 40, 60, 120]]),
    (atr, [{"window": w} for w in [7, 14, 21, 42, 63, 119]]),
    (boll, [{"window": w} for w in [3, 5, 10, 15, 20, 40, 60, 120]]),
    (downside_volatility, [{"window": w} for w in [5, 10, 20, 40, 60, 120]]),
    (upside_downside_ratio, [{"window": w} for w in [5, 10, 20, 40, 60, 120]]),
    (volume_pct, [{"window": w} for w in [5, 10, 15, 20, 40, 60, 120]]),
    (volume_ratio, [{"window": w} for w in [5, 10, 15, 20, 40, 60, 120]]),
    (obv, [{"window": w} for w in [5, 10, 20, 40, 60, 120]]),
    (obv_amount, [{"window": w} for w in [5, 10, 20, 40, 60, 120]]),
    (pvt, [{"window": w} for w in [5, 10, 20, 40, 60, 120]]),
    (amp_vol, [{"window": w} for w in [1, 3, 5, 10, 15, 20, 40, 60, 120]]),
    (amount_amplitude_ratio, [{"window": w} for w in [5, 10, 20, 40, 60, 120]]),
    (amihud_illiquidity, [{"window": w} for w in [5, 10, 20, 40, 60, 120]]),
    (chaikin_money_flow, [{"window": w} for w in [5, 10, 20, 40, 60, 120]]),
    (price_volume_corr, [{"window": w} for w in [5, 10, 20, 40, 60, 120]]),
    (rsi, [{"window": w} for w in [7, 14, 21, 42, 63, 119]]),
    (macd, [{}]),
]
