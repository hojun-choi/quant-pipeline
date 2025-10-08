#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
data/ohlcv/binance-spot/{SYMBOL}/{GRAN}/YYYY-MM-DD.parquet  ->  지표 생성(워밍업 포함 계산)
data/features_all/binance-spot/{SYMBOL}/{GRAN}/YYYY-MM-DD.parquet

- pandas-ta 대형 세트(dict 리스트) + (옵션) 바이낸스 커스텀
- ▶ 워밍업: 이전 날짜 파일에서 필요한 행수만큼 이어붙여 계산 후, 그날만 잘라 저장
- 이미 결과가 존재하면 스킵(--force로 덮어쓰기)

사용 예)
  python scripts/02_make_features_all.py ^
    --symbols BTCUSDT,ETHUSDT ^
    --start 2023-02-01 --end 2025-09-30 ^
    --with-custom ^
    --granularity 1s ^
    --in-root data/ohlcv/binance-spot ^
    --out-root data/features_all/binance-spot
"""

import os
import sys
import glob
import argparse
import shutil
from datetime import datetime, timezone, timedelta
import time

import polars as pl

# ===== 프로젝트 루트 경로 주입 =====
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from features.ta_bridge import run_pandasta_on_polars  # noqa: E402
from features.strategies_all import full_ohlcv_specs   # noqa: E402
from features.custom import add_binance_custom         # noqa: E402


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def atomic_replace(src: str, dst: str):
    """Windows에서도 안전하게 교체"""
    dst = os.path.abspath(dst)
    ensure_dir(os.path.dirname(dst))
    for _ in range(5):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            time.sleep(0.3)
    if os.path.exists(dst):
        os.remove(dst)
    shutil.move(src, dst)


def list_symbols(in_root: str):
    if not os.path.exists(in_root):
        return []
    # 심볼 디렉터리만 취득(하위에 granularity 폴더가 존재한다고 가정)
    out = []
    for d in os.listdir(in_root):
        p = os.path.join(in_root, d)
        if os.path.isdir(p):
            out.append(d)
    return sorted(out)


def ymd_from_fp(fp: str) -> str:
    return os.path.basename(fp).replace(".parquet", "")


def prev_ymd(ymd: str) -> str:
    d = datetime.strptime(ymd, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    p = d - timedelta(days=1)
    return p.strftime("%Y-%m-%d")


def max_window_from_specs(ta_list: list, custom_windows=(60, 300, 900)) -> int:
    """
    지표 스펙에서 길이 후보를 추출해 최대값 반환 (1초봉 기준 '행 수'로 사용).
    커스텀 윈도우(예: rv_900)도 포함.
    """
    keys = {"length", "slow", "fast", "upper_length", "lower_length", "signal", "long", "short", "k", "d", "middle"}
    m = 1
    for spec in ta_list:
        if not isinstance(spec, dict):
            continue
        for k, v in spec.items():
            if k in keys and isinstance(v, int):
                m = max(m, v)
        # 일부 고정값: ichimoku 기본 26
        if spec.get("kind") == "ichimoku":
            m = max(m, 26)
    m = max(m, *(custom_windows or []))
    # 여유 버퍼
    return int(m) + 5


def in_path_for(in_root: str, symbol: str, gran: str, ymd: str) -> str:
    return os.path.join(in_root, symbol, gran, f"{ymd}.parquet") if gran else os.path.join(in_root, symbol, f"{ymd}.parquet")


def out_path_for(out_root: str, symbol: str, gran: str, ymd: str) -> str:
    return os.path.join(out_root, symbol, gran, f"{ymd}.parquet") if gran else os.path.join(out_root, symbol, f"{ymd}.parquet")


def load_with_warmup(in_root: str, symbol: str, gran: str, ymd: str, warmup_rows: int) -> pl.DataFrame:
    """
    해당 일자의 DF를 로드하되, 직전 날짜 파일에서 warmup_rows만큼 이어붙여 반환.
    이전 파일이 없으면 그 일자만 반환.
    """
    cur_path = in_path_for(in_root, symbol, gran, ymd)
    df_cur = pl.read_parquet(cur_path)

    if warmup_rows <= 0:
        return df_cur

    prev_path = in_path_for(in_root, symbol, gran, prev_ymd(ymd))
    if os.path.exists(prev_path):
        df_prev = pl.read_parquet(prev_path)
        if warmup_rows < len(df_prev):
            df_prev = df_prev.tail(warmup_rows)
        df = pl.concat([df_prev, df_cur], how="vertical", rechunk=True)
        return df
    else:
        return df_cur


def slice_to_day(df: pl.DataFrame, ymd: str) -> pl.DataFrame:
    """open_time(ms)를 UTC로 변환해 해당 날짜 구간만 필터링"""
    day = datetime.strptime(ymd, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_ms = int(day.timestamp() * 1000)
    end_ms   = int((day + timedelta(days=1)).timestamp() * 1000) - 1
    return df.filter((pl.col("open_time") >= start_ms) & (pl.col("open_time") <= end_ms))


def process_one(in_root: str, out_root: str, symbol: str, gran: str,
                ymd: str, ta_name: str, ta_list: list,
                with_custom: bool, force: bool, warmup_rows: int):
    in_path  = in_path_for(in_root, symbol, gran, ymd)
    out_path = out_path_for(out_root, symbol, gran, ymd)
    ensure_dir(os.path.dirname(out_path))

    if not os.path.exists(in_path):
        print(f"[{symbol}] {ymd} input missing → skip")
        return

    if os.path.exists(out_path) and not force:
        print(f"[{symbol}] {ymd} exists → skip")
        return

    print(f"[{symbol}] {ymd} loading with warmup({warmup_rows}) from {in_path}")
    df_in = load_with_warmup(in_root, symbol, gran, ymd, warmup_rows=warmup_rows)

    # 1) pandas-ta 지표 계산 (워밍업 포함)
    df_feat = run_pandasta_on_polars(df_in, ta_list=ta_list, name=ta_name)

    # 2) (선택) 바이낸스 커스텀
    if with_custom:
        df_feat = add_binance_custom(df_feat, windows=(60, 300, 900))

    # 3) 해당 날짜만 슬라이스해서 저장
    df_day = slice_to_day(df_feat, ymd)
    tmp_path = out_path + ".tmp"
    df_day.write_parquet(tmp_path, compression="zstd")
    atomic_replace(tmp_path, out_path)
    print(f"[{symbol}] {ymd} → saved {out_path}  rows={len(df_day)}  cols={len(df_day.columns)}")


def main():
    ap = argparse.ArgumentParser(description="Make ALL indicators per day with warmup across days")
    ap.add_argument("--symbols", type=str, default="", help="Comma-separated symbols. Empty: auto-detect under --in-root")
    ap.add_argument("--start", type=str, default="", help="YYYY-MM-DD inclusive (optional)")
    ap.add_argument("--end",   type=str, default="", help="YYYY-MM-DD inclusive (optional)")
    ap.add_argument("--with-custom", action="store_true", help="Add Binance custom features")
    ap.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    ap.add_argument("--warmup", type=int, default=-1, help="Warmup rows (override). Default: auto by indicators")
    # 경로 & 그라뉼러리티
    ap.add_argument("--in-root",  type=str, default="data/ohlcv/binance-spot", help="입력 루트")
    ap.add_argument("--out-root", type=str, default="data/features_all/binance-spot", help="출력 루트")
    ap.add_argument("--granularity", type=str, default="1s", help="하위 폴더명(예: 1s). 빈 문자열이면 생략")

    args = ap.parse_args()

    ta_name, ta_list = full_ohlcv_specs()
    warmup_rows = args.warmup if args.warmup >= 0 else max_window_from_specs(ta_list, custom_windows=(60, 300, 900))

    in_root  = args.in_root
    out_root = args.out_root
    gran     = (args.granularity or "").strip()

    # 심볼 결정
    symbols = ([s.strip().upper() for s in args.symbols.split(",") if s.strip()]
               if args.symbols.strip() else list_symbols(in_root))
    if not symbols:
        print(f"No symbols found under {in_root}")
        sys.exit(1)

    for sym in symbols:
        # 입력 파일 목록 수집
        if gran:
            pattern = os.path.join(in_root, sym, gran, "*.parquet")
        else:
            pattern = os.path.join(in_root, sym, "*.parquet")
        files = sorted(glob.glob(pattern))

        if args.start:
            files = [fp for fp in files if ymd_from_fp(fp) >= args.start]
        if args.end:
            files = [fp for fp in files if ymd_from_fp(fp) <= args.end]
        if not files:
            print(f"[{sym}] no files to process")
            continue

        for fp in files:
            ymd = ymd_from_fp(fp)
            try:
                process_one(in_root, out_root, sym, gran, ymd,
                            ta_name, ta_list,
                            with_custom=args.with_custom,
                            force=args.force,
                            warmup_rows=warmup_rows)
            except KeyboardInterrupt:
                print("\nInterrupted."); sys.exit(1)
            except Exception as e:
                print(f"[{sym}] ERROR {ymd}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
