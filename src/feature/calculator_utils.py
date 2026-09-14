import itertools

import numpy as np
import pandas as pd


def combine_stock_basics(stock_data_df: pd.DataFrame, stock_info_df: pd.DataFrame):
    """
    Gather all of the basic data from the original, unmodified dataset.
    selected fields from stock_data_df: ts_code, trade_date, turnover, pb, pe, roe
    selected fields from stock_info_df: sub_industry
    output columns: ts_code, trade_date, turnover, industry

    Args:
        stock_data_df: single stock's raw data
        stock_info_df: single stock's info dataframe
    Returns:
        combined data frame
    """
    result = pd.DataFrame(index=stock_data_df.index)
    result[["ts_code", "turnover", "pb", "pe", "roe"]] = stock_data_df[
        ["ts_code", "turnover", "pb", "pe", "roe"]
    ]
    result["industry"] = stock_info_df["sub_industry"].iloc[0]
    return result


def compute_price_momentum(
    df: pd.DataFrame, periods: list[int] | None = None
) -> pd.DataFrame:
    """
    Calculate price change related features,
    including `ma_{period}`, `ma_{period}_bias`, `ma_{short}_{long}_ratio` `return_{period}d`
    `raise_days`, `fall_days`, `up_ratio_5d`, `up_ratio_20d`

    Args:
        df: single stock data, with index of trade_date
        periods: a list of periods, sorted increasingly. Normally ignore this param except really need
    Returns:
        pd.DataFrame of calculated features
    """
    if periods is None:
        periods = [5, 10, 20, 60]
    result = pd.DataFrame(index=df.index)
    close = df["adj_close"]
    for period in periods:
        ma = close.rolling(window=period, min_periods=1).mean()
        result[f"ma_{period}"] = (
            (
                ma  # ma will not be in the feature colletion, it will be used for calculation or data visualization
            ).astype("float32")
        )
        result[f"ma_{period}_bias"] = ((close - ma) / ma).astype("float32")
        result[f"return_{period}d"] = (close / close.shift(period) - 1).astype(
            "float32"
        )
    for short, long in itertools.pairwise(periods):
        result[f"ma_{short}_{long}_ratio"] = (
            result[f"ma_{short}"] / result[f"ma_{long}"]
        ).astype("float32")

    # Count consecutive up/down closes. A flat close breaks either streak.
    delta = close.diff()
    status = pd.Series(
        np.where(delta > 0, 1, np.where(delta < 0, -1, 0)), index=df.index
    )
    result["up_ratio_5d"] = (
        status.eq(1).rolling(window=5, min_periods=5).mean().astype("float32")
    )
    result["up_ratio_20d"] = (
        status.eq(1).rolling(window=20, min_periods=20).mean().astype("float32")
    )
    return result


def compute_volume_momentum(df: pd.DataFrame, periods: list[int] | None = None):
    """
    Calculate volume change features
    including `vol_ma_{period}`, `vol_ma_{period}_bias`, `vol_change_{perido}d`
    `vol_ma_{short}_{long}_ratio`

    Args:
        df: single stock data, with index of trade_date
        periods: a list of periods, sorted increasingly. Normally ignore this param except really need
    Returns:
        pd.DataFrame of calculated features
    """
    if periods is None:
        periods = [5, 10, 20, 60]
    result = pd.DataFrame(index=df.index)
    volume = df["adj_vol"]
    for period in periods:
        ma = volume.rolling(window=period, min_periods=1).mean()
        result[f"vol_ma_{period}"] = ma
        result[f"vol_ma_{period}_bias"] = ((volume - ma) / ma).astype("float32")
        result[f"vol_change_{period}d"] = (volume / volume.shift(period) - 1).astype(
            "float32"
        )

    for short, long in itertools.pairwise(periods):
        result[f"vol_ma_{short}_{long}_ratio"] = (
            result[f"vol_ma_{short}"] / result[f"vol_ma_{long}"]
        ).astype("float32")
    return result


def compute_activity_features(
    df: pd.DataFrame, periods: list[int] | None = None, quantile_period: int = 60
) -> pd.DataFrame:
    """
    Calculate market activity related features
    raw or smoothed features: `amplitude`, `amplitude_ma_{period}`, `turnover_ma_{period}_bias`,
    quantile features: `amplitude_quantile_{quantile_period}`, `turnover_quantile_{quantile_period}`
    market activity scores:
    `activity_score_{quantile_period}`, `thin_trade_amplitude_score_{quantile_period},
    `turnover_without_move_score_{quantile_period}`

    Args:
        df: single stock data, with index of `trade_date`
        periods: a list of periods, should sorted increasingly
        quantile_period: an integer period(window) for the quantile feature
    Returns:
        pd.DataFrame of calculated features
    """
    if periods is None:
        periods = [5, 20]
    high = df["adj_high"]
    low = df["adj_low"]
    close = df["adj_close"]
    turnover = df["turnover"]
    prev_close = close.shift(1)
    result = pd.DataFrame(index=df.index)

    amplitude = (high - low) / prev_close.where(prev_close > 0)
    result["amplitude"] = amplitude.astype("float32")

    for period in periods:
        result[f"amplitude_ma_{period}"] = (
            amplitude.rolling(window=period, min_periods=1).mean().astype("float32")
        )

        turnover_ma = turnover.rolling(window=period, min_periods=1).mean()
        result[f"turnover_ma_{period}_bias"] = (
            turnover / turnover_ma.clip(lower=1e-6) - 1
        ).astype("float32")
    amplitude_quantile = amplitude.rolling(window=quantile_period, min_periods=40).rank(
        pct=True
    )
    turnover_quantile = turnover.rolling(window=quantile_period, min_periods=40).rank(
        pct=True
    )
    result[f"amplitude_quantile_{quantile_period}"] = amplitude_quantile.astype(
        "float32"
    )
    result[f"turnover_quantile_{quantile_period}"] = turnover_quantile.astype("float32")

    # high intraday range and high turnover: active trading with elevated price dispersion.
    result[f"activity_score_{quantile_period}"] = (
        amplitude_quantile * turnover_quantile
    ).astype("float32")
    # high intraday range and low turnover: a large price move occurring on relatively light trading.
    result[f"thin_trade_amplitude_score_{quantile_period}"] = amplitude_quantile * (
        1 - turnover_quantile
    ).astype("float32")
    # low intraday range and high turnover: heavy trading activity with limited price movement.
    result[f"turnover_without_move_score_{quantile_period}"] = (
        (1 - amplitude_quantile) * turnover_quantile
    ).astype("float32")

    return result


def compute_volatility(
    df: pd.DataFrame, window: int = 20, rank_period: int = 60
) -> pd.DataFrame:
    """
    Calculate single stock's volatility, to detect risks
    features: `volatility_log`, `volatility_rank`

    Args:
        df: single stock data
    Returns:
        pd.DataFrame with features
    """
    result = pd.DataFrame(index=df.index)
    close = df["adj_close"]
    returns = close.pct_change()
    volatility = returns.rolling(window=window).std()
    result["volatility_log"] = np.log(volatility + 1e-8).astype(
        "float32"
    )  # add 1e-8 to prevent log(0) = -inf
    result["volatility_rank"] = (
        volatility.rolling(window=rank_period).rank(pct=True).astype("float32")
    )
    return result


def compute_cci(
    df: pd.DataFrame, window: int = 14, periods: list[int] | None = None
) -> pd.DataFrame:
    """
    Calculate cci (Commodity Channel Index)
    features `cci`

    Args:
        df: single stock data
    Returns:
        pd.DataFrame with calculated features
    """
    result = pd.DataFrame(index=df.index)
    tp = (df["adj_high"] + df["adj_low"] + df["adj_close"]) / 3
    tp_sma = tp.rolling(window=window, min_periods=window).mean()
    result["tp"] = tp.astype("float32")
    result["tp_sma"] = tp_sma.astype("float32")
    # mean deviation
    md = tp.rolling(window=window, min_periods=window).apply(
        lambda values: np.mean(np.abs(values - values.mean())), raw=True
    )
    # 0.015 was defined by the CCI proposer Donald Lambert
    cci = (tp - tp_sma) / (0.015 * md.replace(0, np.nan))
    result["cci"] = cci.astype("float32")

    # cci moving average
    if periods is None:
        periods = [5]
    for p in periods:
        result[f"cci_ma_{p}"] = (
            cci.rolling(window=p, min_periods=p).mean().astype("float32")
        )
    return result


def compute_dema(df: pd.DataFrame, periods: list[int] | None = None) -> pd.DataFrame:
    """
    Calculate DEMA-related features
    features:
        dema_{fast_period}_{slow_period}_spread
        dema_{fast_period}_{slow_period}_gold
        dema_{fast_period}_{slow_period}_dead

    Args:
        df: single stock data
        periods: a list of moving window period in integer
    Returns:
        pd.DataFrame with calculated features
    """
    result = pd.DataFrame(index=df.index)
    close = df["adj_close"]
    if periods is None:
        periods = [5, 10, 20, 60]
    for fast, slow in itertools.pairwise(periods):
        ema1_fast = close.ewm(span=fast, adjust=False).mean()
        ema1_slow = close.ewm(span=slow, adjust=False).mean()

        ema2_fast = ema1_fast.ewm(span=fast, adjust=False).mean()
        ema2_slow = ema1_slow.ewm(span=slow, adjust=False).mean()

        dema_fast = 2 * ema1_fast - ema2_fast
        dema_slow = 2 * ema1_slow - ema2_slow

        spread = dema_fast / dema_slow - 1

        result[f"dema_{fast}_{slow}_fast"] = dema_fast.astype("float32")
        result[f"dema_{fast}_{slow}_slow"] = dema_slow.astype("float32")
        result[f"dema_{fast}_{slow}_spread"] = spread.astype("float32")
        result[f"dema_{fast}_{slow}_gold"] = (
            (spread.shift(1) <= 0) & (spread > 0)
        ).astype(int)
        result[f"dema_{fast}_{slow}_dead"] = (
            (spread.shift(1) >= 0) & (spread < 0)
        ).astype(int)
    return result


def compute_macd(
    df: pd.DataFrame,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> pd.DataFrame:
    """
    Calculate MACD related features
    features: `macd_diff`, `macd_hist`, `macd_gold`, `macd_dead`

    Args:
        df: a single stock dataframe
        fast_period: integer represents fast period
        slow_period: integer represents slow period, should be larger than the fast period
        signal_period: integer for dea period
    Returns:
        pd.DataFrame with calculated features
    """
    result = pd.DataFrame(index=df.index)
    close = df["adj_close"]

    ema_fast = close.ewm(span=fast_period, adjust=False).mean()
    ema_slow = close.ewm(span=slow_period, adjust=False).mean()
    diff = (
        ema_fast - ema_slow
    ) / ema_slow  # divide ema_slow for the normalization purpose
    dea = diff.ewm(span=signal_period, adjust=False).mean()
    macd_hist = diff - dea

    result["macd_ema_slow"] = ema_slow.astype("float32")
    result["macd_ema_fast"] = ema_fast.astype("float32")
    result["macd_diff"] = diff.astype("float32")
    result["macd_dea"] = dea.astype("float32")
    result["macd_hist"] = macd_hist.astype("float32")

    macd_gold = ((macd_hist.shift(1) < 0) & (macd_hist > 0)).astype(int)
    macd_dead = ((macd_hist.shift(1) > 0) & (macd_hist < 0)).astype(int)
    result["macd_dead"] = macd_dead
    result["macd_gold"] = macd_gold

    return result


def compute_bollinger_bands(
    df: pd.DataFrame, period: int = 20, std_dev: float = 2.0
) -> pd.DataFrame:
    """
    Calculate stock's Bollinger Bands
    features: `bb_width`, `bb_position`

    Args:
        df: a single stock dataframe
        period:  period for rolling std
        std_dev: scale factor of std

    Return:
        pd.DataFrame of the calculated features
    """
    result = pd.DataFrame(index=df.index)
    close = df["adj_close"]
    mid = close.rolling(window=period).mean()
    rolling_std = close.rolling(window=period).std(ddof=0)  # ddof=0 for population std

    upper = mid + rolling_std * std_dev
    lower = mid - rolling_std * std_dev
    diff_ul = upper - lower

    bb_width = diff_ul / mid.replace(0, np.nan)
    bb_position = (close - lower) / diff_ul.replace(0, np.nan)

    result["bb_upper"] = upper.astype("float32")
    result["bb_lower"] = lower.astype("float32")
    result["bb_mid"] = mid.astype("float32")
    result["bb_width"] = bb_width.astype("float32")
    result["bb_position"] = bb_position.astype("float32")
    return result


def compute_kdj(df: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
    """
    Calculate KDJ (stochastic-oscillator)
    features: `kdj_k`

    Args:
        df: a single stock's dataframe
        n: rolling window
        m1: smooth factor for K line, as an integer
        m2: smooth factor for D line, as an integer

    Returns:
        pd.DataFrame: calculated features
    """
    result = pd.DataFrame(index=df.index)
    low = df["adj_low"]
    high = df["adj_high"]
    close = df["adj_close"]

    # calculate min/max price in n day window
    low_n = low.rolling(window=n).min()
    high_n = high.rolling(window=n).max()

    denominator = high_n - low_n
    rsv = pd.Series(np.nan, index=df.index)
    valid = denominator.notna()
    is_flat = valid & (denominator == 0)
    is_normal = valid & (denominator != 0)
    # RSV: Raw Stochastic Value
    rsv.loc[is_flat] = 50.0
    rsv.loc[is_normal] = (
        (close[is_normal] - low_n[is_normal]) / denominator[is_normal] * 100
    )
    # k: smoothed rsv
    k = rsv.ewm(alpha=1 / m1, adjust=False).mean()
    d = k.ewm(alpha=1 / m2, adjust=False).mean()
    j = 3 * k - 2 * d

    result[f"kdj_low_{n}"] = low_n.astype("float32")
    result[f"kdj_high_{n}"] = high_n.astype("float32")
    result["kdj_k"] = k.astype("float32")
    result["kdj_d"] = d.astype("float32")
    result["kdj_j"] = j.astype("float32")
    return result


def compute_rsi(df: pd.DataFrame, periods: list[int] | None = None) -> pd.DataFrame:
    """
    Calculate rsi (Relative Strength Index)
    features: rsi: rsi_{period}

    Args:
        df: a single stock's dataframe
        periods: a list of period in integer
    Returns:
        pd.DataFrame of calculated features
    """
    if periods == None:
        periods = [6, 14]  # default periods

    result = pd.DataFrame(index=df.index)
    close = df["adj_close"]
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)

    for period in periods:
        avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        result[f"rsi_{period}"] = rsi.astype("float32")
        result[f"rsi_{period}_over_bought"] = (rsi > 70).astype(int)
        result[f"rsi_{period}_over_sold"] = (rsi < 30).astype(int)

    return result


def compute_obv(df: pd.DataFrame, periods: list[int] | None = None) -> pd.DataFrame:
    """
    Calculate obv (On Balance Volume)

    """
    if periods is None:
        periods = [5, 20]
    result = pd.DataFrame(index=df.index)
    close = df["adj_close"]
    volume = df["adj_vol"]

    delta = close.diff()
    signed_volume = volume.where(delta > 0, -volume.where(delta < 0, 0))
    signed_volume.iloc[0] = (
        0  # set the first day as zero to avoid any wrong gain/loss sign
    )
    for period in periods:
        flow = signed_volume.rolling(period, min_periods=period).sum()
        result[f"obv_strength_{period}"] = (
            flow / volume.rolling(period, min_periods=period).sum()
        ).astype("float32")

    for fast, slow in itertools.pairwise(periods):
        # between [2, -2]
        result[f"obv_strength_accel_{fast}_{slow}"] = (
            result[f"obv_strength_{fast}"] - result[f"obv_strength_{slow}"]
        ).astype("float32")
    return result
