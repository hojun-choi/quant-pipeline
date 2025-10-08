# features/ta_bridge.py
import pandas as pd
import polars as pl
import pandas_ta as ta  # noqa: F401  # 일부 내부 참조

def _apply_indicators_inplace(df_pd: pd.DataFrame, ta_list: list, name: str = "FULL_SET") -> pd.DataFrame:
    """pandas-ta 0.4.x: Strategy 없이 dict 리스트로 개별 호출"""
    for i, spec in enumerate(ta_list, 1):
        if not isinstance(spec, dict) or "kind" not in spec:
            print(f"[WARN] skip invalid spec at #{i}: {spec}")
            continue
        kind = spec.get("kind")
        params = {k: v for k, v in spec.items() if k != "kind"}
        params.setdefault("append", True)  # 많은 지표가 append 지원

        func = getattr(df_pd.ta, kind, None)
        if func is None:
            print(f"[WARN] pandas-ta: indicator '{kind}' not found → skip")
            continue

        try:
            _ = func(**params)
        except TypeError as e:
            if "append" in params:
                params2 = {k: v for k, v in params.items() if k != "append"}
                try:
                    _ = func(**params2)
                except Exception as e2:
                    print(f"[WARN] indicator '{kind}' failed without append: {e2} → skip")
            else:
                print(f"[WARN] indicator '{kind}' failed: {e} → skip")
        except Exception as e:
            print(f"[WARN] indicator '{kind}' error: {e} → skip")
    return df_pd


def run_pandasta_on_polars(df_pl: pl.DataFrame, ta_list: list, name: str = "FULL_SET") -> pl.DataFrame:
    """
    Polars DF(OHLCV, open_time(ms)) -> pandas-ta 지표 일괄 추가 -> Polars DF로
    (df_pl에 워밍업 구간이 포함되어 있어도 그대로 계산)
    """
    need_cols = {"open_time", "open", "high", "low", "close", "volume"}
    missing = need_cols - set(df_pl.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    # Polars -> Pandas (DatetimeIndex)
    df_pd = df_pl.select([
        pl.col("open_time").alias("ts_ms"),
        "open", "high", "low", "close", "volume"
    ]).to_pandas()
    df_pd["ts"] = pd.to_datetime(df_pd["ts_ms"], unit="ms", utc=True)
    df_pd = df_pd.set_index("ts").drop(columns=["ts_ms"]).sort_index()

    # 지표 계산
    df_pd = _apply_indicators_inplace(df_pd, ta_list=ta_list, name=name)

    # Pandas -> Polars
    df_pd = df_pd.reset_index()
    df_pd["open_time"] = (df_pd["ts"].astype("int64") // 10**6)  # ns→ms
    df_pd = df_pd.drop(columns=["ts"])
    df_right = pl.from_pandas(df_pd)

    # ▶ 신규 지표 컬럼만 선택해서 조인 (기본 OHLCV는 제외)
    base_cols = {"open_time", "open", "high", "low", "close", "volume"}
    new_cols = [c for c in df_right.columns if c not in base_cols]
    df_right = df_right.select(["open_time", *new_cols])

    # 원본과 조인
    df_out = df_pl.join(df_right, on="open_time", how="left")
    return df_out
