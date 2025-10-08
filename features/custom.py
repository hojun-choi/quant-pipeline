# features/custom.py
import polars as pl

def add_binance_custom(df: pl.DataFrame, windows=(60, 300, 900)) -> pl.DataFrame:
    """
    바이낸스 전용 필드 기반 확장 피처 (오더플로/실현변동성 등)
    """
    need = {"volume", "quote_volume", "taker_buy_base", "taker_buy_quote", "num_trades"}
    if not need.issubset(set(df.columns)):
        return df

    out = df.with_columns([
        (pl.col("taker_buy_base") / pl.when(pl.col("volume") == 0).then(1).otherwise(pl.col("volume"))).alias("tbb_ratio"),
        (pl.col("taker_buy_quote") / pl.when(pl.col("quote_volume") == 0).then(1).otherwise(pl.col("quote_volume"))).alias("tbq_ratio"),
        pl.col("num_trades").alias("intensity"),
        (pl.col("close").log() - pl.col("close").shift(1).log()).alias("r1"),
    ]).with_columns([
        ((pl.col("tbb_ratio") * 2 - 1) * pl.col("volume")).alias("signed_volume")
    ])

    for w in windows:
        out = out.with_columns([
            pl.col("r1").pow(2).rolling_sum(window_size=w).alias(f"rv_{w}"),
            pl.col("intensity").rolling_mean(window_size=w).alias(f"intensity_mean_{w}"),
            pl.col("volume").rolling_mean(window_size=w).alias(f"vol_mean_{w}"),
            pl.col("signed_volume").rolling_mean(window_size=w).alias(f"sv_mean_{w}"),
            (pl.col("high") - pl.col("low")).rolling_mean(window_size=w).alias(f"hl_mean_{w}"),
        ])
    return out
