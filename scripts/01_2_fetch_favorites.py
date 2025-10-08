#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

# 1) 즐겨찾기(기본 자산) — SUI 제외
FAVORITES = [
    "btc","eth","xrp","sol","bnb","doge","trx","ada","link",
    "avax","xlm","bch","ltc","dot"
]

# 2) USDT 페어로 변환
symbols = ",".join([f"{c.upper()}USDT" for c in FAVORITES])

# 3) 기간: 2023-01-01 ~ 어제(UTC)
start_date = "2023-01-01"
yesterday_utc = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()

# 4) 출력 루트 및 호출 스크립트 경로
out_root = "data/ohlcv/binance-spot"
granularity = "1s"
fetch_script = Path(__file__).parent / "01_fetch_ohlcv.py"

# 5) 실행 커맨드 구성
cmd = [
    sys.executable, str(fetch_script),
    "--symbols", symbols,
    "--interval", granularity,
    "--start", start_date,
    "--end", yesterday_utc,      # 내부에서 어제-캡이 또 걸림
    "--out", out_root,
    "--granularity", granularity
]

print("[favorites] Running:", " ".join(cmd))
print(f"[favorites] Effective: symbols={symbols}")
print(f"[favorites] Range(UTC): {start_date} .. {yesterday_utc} (inclusive)")
print(f"[favorites] Output to: {out_root}/<SYMBOL>/{granularity}/YYYY-MM-DD.parquet")

# 6) 실행
completed = subprocess.run(cmd)
sys.exit(completed.returncode)
