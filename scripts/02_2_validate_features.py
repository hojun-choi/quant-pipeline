# scripts/02_2_validate_features.py
import os, sys, json
import pandas as pd
import numpy as np

def load_df(path: str) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.read_parquet(path, engine="pyarrow")

def check_file(path: str, first_n: int = 200):
    df = load_df(path)
    rep = {}
    rep["path"] = path
    rep["shape"] = df.shape

    # 1) 기본 컬럼 확인
    base = ["open_time","open","high","low","close","volume","quote_volume","num_trades","taker_buy_base","taker_buy_quote"]
    rep["missing_base_cols"] = [c for c in base if c not in df.columns]

    # 2) open_time 단조성 & 1초 간격 검증
    if "open_time" in df.columns:
        ot = df["open_time"].to_numpy()
        rep["open_time_monotonic"] = bool(np.all(np.diff(ot) >= 0))
        diffs = np.unique(np.diff(ot))
        rep["open_time_unique_diffs"] = diffs.tolist()[:5]
        rep["open_time_1s_rate"] = float(np.mean(np.diff(ot) == 1000))
    else:
        rep["open_time_monotonic"] = False
        rep["open_time_unique_diffs"] = []
        rep["open_time_1s_rate"] = 0.0

    # 3) '_right' 중복 컬럼 존재 여부
    rep["right_cols"] = [c for c in df.columns if c.endswith("_right")]

    # 4) 전체/초반 결측률
    numeric_cols = [c for c in df.columns if c not in base]
    head = df.head(first_n)
    null_rate_all = df[numeric_cols].isna().mean().sort_values(ascending=False)
    null_rate_head = head[numeric_cols].isna().mean().sort_values(ascending=False)
    # 상위 몇 개만 보고
    rep["top_null_cols_all"] = null_rate_all.head(15).to_dict()
    rep["top_null_cols_head"] = null_rate_head.head(15).to_dict()
    rep["all_null_cols"] = [c for c, r in null_rate_all.items() if r == 1.0]

    # 5) 대표 지표(예: RSI_14, MACD, BBANDS) 유효 샘플 수
    probe = ["RSI_14","MACD_12_26_9","BBM_20_2.0_2.0","VWAP_D","OBV","LOGRET_1","HA_close"]
    for p in probe:
        if p in df.columns:
            rep[f"{p}_notnull_ratio_all"] = float((~df[p].isna()).mean())
            rep[f"{p}_notnull_ratio_head"] = float((~head[p].isna()).mean())

    print(json.dumps(rep, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    for p in sys.argv[1:]:
        check_file(p)
