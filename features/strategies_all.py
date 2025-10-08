# -*- coding: utf-8 -*-
"""
옵션: 간단/경량 버전
- 각 지표별 '대표 길이 3개'만 사용 (멀티타임프레임 스케일링 제거)
- 길이 개념이 없는 지표는 1개만 추가
- pandas-ta 0.4.71b0 기준 파라미터 사용

주의:
- 생성 컬럼명은 pandas-ta 규칙을 따름 (예: RSI_14, MACD_12_26_9 등)
- 무거운 지표(qqe/supertrend/linreg 장창 등)는 일부만 유지
"""

from __future__ import annotations
from typing import List, Dict, Any

def full_ohlcv_specs():
    """
    간단 세트(지표당 3개 길이). 멀티타임프레임 스케일링 없음.
    """
    name = "FULL_SET_SIMPLE"
    ta_list: List[Dict[str, Any]] = []

    # ---- Momentum / Oscillators ----
    ta_list += [{"kind": "rsi", "length": L} for L in [7, 14, 21]]
    # stoch: k/d/smooth_k 3세트
    for k in [9, 14, 21]:
        ta_list += [{"kind": "stoch", "k": k, "d": 3, "smooth_k": 3}]
    ta_list += [{"kind": "stochrsi", "length": L} for L in [14, 21, 42]]

    # MACD 계열
    for (f, s, sig) in [(12, 26, 9), (24, 52, 18), (36, 78, 27)]:
        ta_list += [{"kind": "macd", "fast": f, "slow": s, "signal": sig}]
        ta_list += [{"kind": "ppo",  "fast": f, "slow": s, "signal": sig}]
    ta_list += [{"kind": "apo", "fast": f, "slow": s} for (f, s) in [(12, 26), (24, 52), (36, 78)]]

    ta_list += [{"kind": "roc", "length": L} for L in [6, 12, 24]]
    ta_list += [{"kind": "mom", "length": L} for L in [5, 10, 20]]
    ta_list += [{"kind": "cmo", "length": L} for L in [14, 42, 100]]
    ta_list += [{"kind": "cci", "length": L} for L in [20, 50, 100]]
    ta_list += [{"kind": "er",  "length": L} for L in [10, 20, 40]]

    ta_list += [{"kind": "kst"}]  # 복합내부 고정
    ta_list += [{"kind": "pgo",  "length": L} for L in [14, 21, 42]]
    ta_list += [{"kind": "qqe",  "length": L} for L in [14, 21, 42]]  # 상태형, 초기 NaN 많음 정상
    ta_list += [{"kind": "rvgi", "length": L} for L in [14, 21, 42]]
    ta_list += [{"kind": "smi",  "length": L} for L in [14, 20, 40]]
    ta_list += [{"kind": "stc"}]  # 고정
    ta_list += [{"kind": "trix", "length": L} for L in [9, 18, 36]]
    for (L, S) in [(25, 13), (50, 25), (100, 50)]:
        ta_list += [{"kind": "tsi", "long": L, "short": S}]
    for (f, m, s) in [(7, 14, 28), (14, 28, 56), (21, 42, 84)]:
        ta_list += [{"kind": "uo", "fast": f, "middle": m, "slow": s}]
    ta_list += [{"kind": "willr", "length": L} for L in [14, 42, 100]]
    ta_list += [{"kind": "bop"}]
    ta_list += [{"kind": "cfo", "length": L} for L in [9, 20, 40]]
    ta_list += [{"kind": "cg",  "length": L} for L in [10, 20, 40]]
    ta_list += [{"kind": "cti", "length": L} for L in [12, 24, 48]]
    ta_list += [{"kind": "fisher", "length": L} for L in [5, 9, 13]]
    ta_list += [{"kind": "inertia", "length": L} for L in [14, 20, 40]]

    # ---- Trend ----
    ta_list += [{"kind": "adx",   "length": L} for L in [14, 28, 56]]
    ta_list += [{"kind": "aroon", "length": L} for L in [14, 25, 50]]
    ta_list += [{"kind": "chop",  "length": L} for L in [14, 30, 56]]
    ta_list += [{"kind": "dpo",   "length": L, "lookahead": False} for L in [20, 60, 120]]
    ta_list += [{"kind": "qstick","length": L} for L in [14, 28, 56]]
    ta_list += [{"kind": "vhf",   "length": L} for L in [14, 28, 56]]
    ta_list += [{"kind": "vortex","length": L} for L in [14, 21, 28]]
    ta_list += [{"kind": "psar", "af": 0.02, "max_af": 0.2}]  # 상태형 고정

    # ---- Overlap / Averages / Channels ----
    ta_list += [{"kind": "ema",  "length": L} for L in [12, 26, 50]]
    ta_list += [{"kind": "sma",  "length": L} for L in [20, 50, 100]]
    ta_list += [{"kind": "wma",  "length": L} for L in [21, 50, 100]]
    ta_list += [{"kind": "rma",  "length": L} for L in [14, 50, 100]]
    ta_list += [{"kind": "hma",  "length": L} for L in [21, 55, 89]]
    ta_list += [{"kind": "t3",   "length": L} for L in [5, 10, 20]]
    ta_list += [{"kind": "tema", "length": L} for L in [9, 30, 50]]
    ta_list += [{"kind": "dema", "length": L} for L in [9, 30, 50]]
    ta_list += [{"kind": "vwma", "length": L} for L in [20, 50, 100]]
    ta_list += [{"kind": "vwap"}]  # 길이 없음

    # BBANDS(length, std)
    for n in [20, 50, 100]:
        ta_list += [{"kind": "bbands", "length": n, "std": 2.0}]
    # KC(length)
    ta_list += [{"kind": "kc", "length": L} for L in [20, 40, 60]]
    # Donchian(lower=upper=L)
    for L in [20, 55, 100]:
        ta_list += [{"kind": "donchian", "lower_length": L, "upper_length": L}]

    # Supertrend(length, multiplier)
    for (L, M) in [(10, 3), (20, 2), (7, 4)]:
        ta_list += [{"kind": "supertrend", "length": L, "multiplier": M}]

    ta_list += [{"kind": "linreg",   "length": L} for L in [20, 60, 120]]
    ta_list += [{"kind": "midpoint", "length": L} for L in [14, 42, 100]]
    ta_list += [{"kind": "midprice", "length": L} for L in [14, 42, 100]]
    ta_list += [{"kind": "ichimoku", "lookahead": False}]  # 고정

    # ---- Volatility / Range ----
    ta_list += [{"kind": "atr",   "length": L} for L in [14, 42, 100]]
    ta_list += [{"kind": "true_range"}]
    ta_list += [{"kind": "stdev", "length": L} for L in [20, 60, 120]]
    ta_list += [{"kind": "natr",  "length": L} for L in [14, 42, 100]]
    ta_list += [{"kind": "accbands", "length": L} for L in [20, 50, 100]]
    ta_list += [{"kind": "aberration", "length": L} for L in [20, 50, 100]]

    # ---- Volume Family ----
    ta_list += [{"kind": "ad"}]
    for (f, s) in [(3, 10), (5, 20), (8, 34)]:
        ta_list += [{"kind": "adosc", "fast": f, "slow": s}]
    ta_list += [{"kind": "obv"}]
    ta_list += [{"kind": "mfi", "length": L} for L in [14, 28, 56]]
    ta_list += [{"kind": "cmf", "length": L} for L in [20, 50, 100]]
    # KVO(fast, slow, signal)
    for (f, s, g) in [(34, 55, 13), (55, 89, 13), (21, 34, 8)]:
        ta_list += [{"kind": "kvo", "fast": f, "slow": s, "signal": g}]
    ta_list += [{"kind": "aobv", "length": L} for L in [7, 14, 28]]
    ta_list += [{"kind": "pvi"}]
    ta_list += [{"kind": "nvi"}]
    ta_list += [{"kind": "pvt"}]

    # ---- Performance / Returns ----
    ta_list += [{"kind": "log_return", "cumulative": False}]
    ta_list += [{"kind": "percent_return", "cumulative": False}]

    # ---- Statistics ----
    ta_list += [{"kind": "entropy",  "length": L} for L in [60, 300, 900]]
    ta_list += [{"kind": "kurtosis", "length": L} for L in [60, 300, 900]]
    ta_list += [{"kind": "skew",     "length": L} for L in [60, 300, 900]]
    ta_list += [{"kind": "zscore",   "length": L} for L in [60, 300, 900]]
    ta_list += [{"kind": "mad",      "length": L} for L in [60, 300, 900]]
    ta_list += [{"kind": "variance", "length": L} for L in [60, 300, 900]]

    # ---- Candles / Transforms ----
    ta_list += [{"kind": "ha"}]
    ta_list += [{"kind": "ohlc4"}]
    ta_list += [{"kind": "hl2"}]
    ta_list += [{"kind": "hlc3"}]

    return name, ta_list

# (과거 호환을 위해 이름만 다른 동일 세트가 필요하면 아래를 풀어 쓰면 됩니다)
def full_ohlcv_paramgrid_scaled():
    # 예전 코드가 이 이름을 호출하더라도 동일한 간단 세트를 반환하게 함
    return full_ohlcv_specs()
