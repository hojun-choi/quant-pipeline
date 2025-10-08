#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
즐겨찾기 코인들(USDT 페어)에 대해
2023-02-01 ~ 2025-09-30 범위의 보조지표(02_make_features_all.py)를 일괄 생성.

- 입력:  data/ohlcv/binance-spot/<SYMBOL>/1s/YYYY-MM-DD.parquet
- 출력:  data/features_all/binance-spot/<SYMBOL>/1s/YYYY-MM-DD.parquet
- 기본: Binance 커스텀 피처 포함(--with-custom)
- 워밍업(--warmup)은 None이면 02_make_features_all.py 내부 기본값 사용
- 이미 있는 출력은 스킵(덮어쓰려면 --force)

사용 예)
  # 기본 옵션(커스텀 포함, 덮어쓰기 없음)
  python scripts/02_3_make_features_favorites.py

  # 덮어쓰기 + 워밍업 2000 지정
  python scripts/02_3_make_features_favorites.py --force --warmup 2000
"""

import sys
import argparse
import subprocess
from pathlib import Path

# 1) 즐겨찾기 (SUI 제외)
FAVORITES = [
    "btc","eth","xrp","sol","bnb","doge","trx","ada","link",
    "avax","xlm","bch","ltc","dot"
]

# 2) USDT 페어로 변환 (쉼표 구분)
SYMBOLS = ",".join(f"{c.upper()}USDT" for c in FAVORITES)

# 3) 기간 고정
START_DATE = "2023-02-01"
END_DATE   = "2025-09-30"

# 4) 경로/세부 옵션
GRANULARITY = "1s"  # 하위 폴더명
IN_ROOT  = "data/ohlcv/binance-spot"
OUT_ROOT = "data/features_all/binance-spot"

# 5) 호출 스크립트 경로
THIS_DIR = Path(__file__).parent
MAKE_FEATS = THIS_DIR / "02_make_features_all.py"


def build_cmd(with_custom: bool, force_overwrite: bool, warmup_rows: int|None):
    cmd = [
        sys.executable, str(MAKE_FEATS),
        "--symbols", SYMBOLS,
        "--start", START_DATE,
        "--end", END_DATE,
        "--with-custom",                     # 기본 포함
        "--granularity", GRANULARITY,        # <SYMBOL>/<GRANULARITY>/YYYY-MM-DD.parquet
        "--in-root", IN_ROOT,                # 입력 루트
        "--out-root", OUT_ROOT,              # 출력 루트
    ]
    if not with_custom:
        # --no-custom 요청 시 with-custom 제거
        cmd.remove("--with-custom")
    if force_overwrite:
        cmd.append("--force")
    if isinstance(warmup_rows, int) and warmup_rows >= 0:
        cmd += ["--warmup", str(warmup_rows)]
    return cmd


def main():
    ap = argparse.ArgumentParser(description="Make features for favorite symbols over a fixed date range.")
    ap.add_argument("--no-custom", action="store_true", help="커스텀 피처 제외(기본은 포함).")
    ap.add_argument("--force", action="store_true", help="기존 결과 덮어쓰기.")
    ap.add_argument("--warmup", type=int, default=None, help="워밍업 행 수(미지정 시 자동).")
    args = ap.parse_args()

    with_custom = (not args.no_custom)
    force_overwrite = args.force
    warmup_rows = args.warmup

    cmd = build_cmd(with_custom, force_overwrite, warmup_rows)

    print("[features-favorites] Running:\n ", " ".join(cmd))
    print(f"[features-favorites] symbols={SYMBOLS}")
    print(f"[features-favorites] range={START_DATE}..{END_DATE} (UTC, inclusive)")
    print(f"[features-favorites] in_root={IN_ROOT}  out_root={OUT_ROOT}  granularity={GRANULARITY}")
    print(f"[features-favorites] with_custom={with_custom}  force={force_overwrite}  warmup={warmup_rows}")

    rc = subprocess.run(cmd).returncode
    sys.exit(rc)


if __name__ == "__main__":
    main()
