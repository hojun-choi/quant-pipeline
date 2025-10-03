#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

# 1) 네가 원하는 코인 목록 (기본 자산 기호만)
FAVORITES = [
    "btc","eth","xrp","sol","bnb","doge","trx","ada","link",
    "avax","sui","xlm","bch","ltc","dot"
]

# 2) USDT 페어로 변환
symbols = ",".join([f"{c.upper()}USDT" for c in FAVORITES])

# 3) 기간: 2023-01-01 ~ 어제(UTC)
start_date = "2023-01-01"
yesterday_utc = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()  # 예: '2025-10-02'

# 4) 출력 루트 및 호출 스크립트 경로
out_root = "data/ohlcv/binance-spot"
fetch_script = Path(__file__).parent / "01_fetch_ohlcv.py"

# 5) 실행 커맨드 구성
cmd = [
    sys.executable, str(fetch_script),
    "--symbols", symbols,
    "--interval", "1s",
    "--start", start_date,
    "--end", yesterday_utc,      # 내부에서 어제-캡이 또 걸리므로 안전
    "--out", out_root,
]

print("[favorites] Running:", " ".join(cmd))
print(f"[favorites] Effective: symbols={symbols}  range={start_date}..{yesterday_utc} (UTC, inclusive)")
print(f"[favorites] Output to: {out_root}")

# 6) 실행
completed = subprocess.run(cmd)
sys.exit(completed.returncode)
