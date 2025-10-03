#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, time, argparse, tempfile, shutil
from datetime import datetime, timezone, timedelta, date
from typing import Dict, Any, List

import requests
import polars as pl

BINANCE_API = "https://api.binance.com"
KLINES_PATH = "/api/v3/klines"  # spot

# ---------- utils ----------

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def atomic_replace(src_path: str, dst_path: str):
    dst_path = os.path.abspath(dst_path)
    ensure_dir(os.path.dirname(dst_path))
    for _ in range(5):
        try:
            os.replace(src_path, dst_path)
            return
        except PermissionError:
            time.sleep(0.5)
    try:
        if os.path.exists(dst_path):
            os.remove(dst_path)
        shutil.move(src_path, dst_path)
    finally:
        if os.path.exists(src_path):
            os.remove(src_path)

def atomic_write_parquet(df: pl.DataFrame, out_path: str, compression: str = "zstd", level: int = 5):
    out_dir = os.path.abspath(os.path.dirname(out_path))
    ensure_dir(out_dir)
    tmp = tempfile.NamedTemporaryFile("wb", suffix=".parquet", delete=False, dir=out_dir)
    tmp_path = tmp.name
    tmp.close()
    try:
        df.write_parquet(tmp_path, compression=compression, compression_level=level)
        atomic_replace(tmp_path, out_path)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

def parse_utc_date(s: str) -> date:
    s = s.strip()
    if s.lower() == "yesterday":
        # placeholder, 실제 날짜 계산은 main에서
        return None  # type: ignore
    if "T" in s:
        s2 = s.replace(" ", "T")
        if s2.endswith("Z"):
            s2 = s2.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.date()
    y, m, d = map(int, s.split("-"))
    return date(y, m, d)

def start_of_day_utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)

def end_of_day_utc(d: date) -> datetime:
    return start_of_day_utc(d) + timedelta(days=1) - timedelta(milliseconds=1)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def utc_to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

def interval_to_ms(interval: str) -> int:
    iv = interval.strip().lower()
    unit = iv[-1]
    value = int(iv[:-1])
    if unit == "s": return value * 1000
    if unit == "m": return value * 60_000
    if unit == "h": return value * 3_600_000
    if unit == "d": return value * 86_400_000
    if unit == "w": return value * 7 * 86_400_000
    raise ValueError(f"Unsupported interval: {interval}")

# ---------- Binance API ----------

class RateLimiter:
    def __init__(self, target_per_minute: int = 5000, safety_margin: int = 200):
        self.target = target_per_minute
        self.margin = safety_margin
        self.backoff = 1.0
    def handle_headers(self, headers: Dict[str, str]):
        key = next((k for k in headers.keys() if k.lower() == "x-mbx-used-weight-1m"), None)
        if not key: return
        try:
            used = int(headers[key])
            if used >= (self.target - self.margin):
                time.sleep(1.5)
        except: pass
    def on_429(self):
        time.sleep(self.backoff)
        self.backoff = min(self.backoff * 2, 30.0)
    def reset(self):
        self.backoff = 1.0

def fetch_klines(sess: requests.Session, symbol: str, interval: str,
                 start_ms: int, end_ms: int, limit: int, rl: RateLimiter) -> List[List[Any]]:
    url = BINANCE_API + KLINES_PATH
    all_rows: List[List[Any]] = []
    cur = start_ms
    step_ms = interval_to_ms(interval) * limit

    while cur <= end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cur,
            "endTime": end_ms,
            "limit": limit,
        }
        for _ in range(8):
            try:
                r = sess.get(url, params=params, timeout=20)
                if r.status_code == 200:
                    rl.handle_headers(r.headers); rl.reset()
                    rows = r.json()
                    if not rows:
                        cur = min(cur + step_ms, end_ms + 1)
                        break
                    all_rows.extend(rows)
                    last_close = int(rows[-1][6])
                    cur = max(last_close + 1, cur + 1)
                    break
                elif r.status_code in (418, 429):
                    rl.on_429()
                else:
                    time.sleep(0.8)
            except requests.RequestException:
                rl.on_429()
        else:
            raise RuntimeError(f"klines request failed repeatedly: {symbol} {interval} {cur}-{end_ms}")

    return all_rows

def rows_to_df(symbol: str, rows: List[List[Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={
            "open_time": pl.Int64, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
            "volume": pl.Float64, "close_time": pl.Int64, "quote_volume": pl.Float64,
            "num_trades": pl.Int64, "taker_buy_base": pl.Float64, "taker_buy_quote": pl.Float64,
            "exchange": pl.Utf8, "symbol": pl.Utf8
        })
    cols = {
        "open_time": [int(r[0]) for r in rows],
        "open": [float(r[1]) for r in rows],
        "high": [float(r[2]) for r in rows],
        "low": [float(r[3]) for r in rows],
        "close": [float(r[4]) for r in rows],
        "volume": [float(r[5]) for r in rows],
        "close_time": [int(r[6]) for r in rows],
        "quote_volume": [float(r[7]) for r in rows],
        "num_trades": [int(r[8]) for r in rows],
        "taker_buy_base": [float(r[9]) for r in rows],
        "taker_buy_quote": [float(r[10]) for r in rows],
    }
    return pl.DataFrame(cols).with_columns([
        pl.lit("binance-spot").alias("exchange"),
        pl.lit(symbol).alias("symbol"),
    ])

# ---------- per-day ingest (no checkpoints) ----------

def ingest_one_day(symbol: str, interval: str, d: date, out_root: str,
                   limit: int, target_weight_per_minute: int, force: bool = False):
    out_dir = os.path.join(out_root, symbol); ensure_dir(out_dir)
    out_path = os.path.join(out_dir, f"{d.strftime('%Y-%m-%d')}.parquet")
    if os.path.exists(out_path) and not force:
        print(f"[{symbol}] {d} exists → skip"); return

    day_start = start_of_day_utc(d); day_end = end_of_day_utc(d)
    s_ms, e_ms = utc_to_ms(day_start), utc_to_ms(day_end)

    print(f"[{symbol}] fetching {interval} for {d} (UTC {day_start} ~ {day_end})")
    rl = RateLimiter(target_per_minute=target_weight_per_minute, safety_margin=200)
    sess = requests.Session()

    rows = fetch_klines(sess, symbol, interval, s_ms, e_ms, limit, rl)
    if not rows:
        print(f"[{symbol}] WARNING: no rows for {d}"); return

    df = rows_to_df(symbol, rows)
    df = df.filter((pl.col("open_time") >= s_ms) & (pl.col("close_time") <= e_ms))
    df = df.unique(subset=["open_time"], keep="last").sort("open_time")

    # (info) 기대 개수 안내
    try:
        iv_ms = interval_to_ms(interval)
        expected = int(((e_ms - s_ms + 1) // iv_ms))
        got = df.height
        if expected and got != expected:
            print(f"[{symbol}] NOTE: {d} expected≈{expected}, got={got}")
    except Exception:
        pass

    atomic_write_parquet(df, out_path, compression="zstd", level=5)
    print(f"[{symbol}] saved {out_path}  rows={df.height}")

# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(
        description="Per-day Binance Spot klines (no checkpoints). End date is capped to yesterday(UTC) unless --allow-today. You can also fix the reference time via --now."
    )
    ap.add_argument("--symbols", type=str, required=True, help="Comma-separated symbols, e.g., BTCUSDT,ETHUSDT")
    ap.add_argument("--interval", type=str, default="1s", help="e.g., 1s,5s,10s,30s,1m,5m,1h")
    ap.add_argument("--start", type=str, required=True, help="UTC date or ISO; date part used. e.g., 2024-10-01")
    ap.add_argument("--end", type=str, default=None, help="UTC date inclusive OR 'yesterday'. If omitted, yesterday(UTC).")
    ap.add_argument("--out", type=str, default="data/ohlcv/binance-spot", help="Output root")
    ap.add_argument("--limit", type=int, default=1000, help="klines page size (<=1000)")
    ap.add_argument("--weight", type=int, default=5000, help="Target used-weight per minute")
    ap.add_argument("--force", action="store_true", help="Overwrite even if the daily file exists")
    ap.add_argument("--allow-today", action="store_true", help="Do NOT cap end date to yesterday(UTC)")
    ap.add_argument("--now", type=str, default=None, help="Reference UTC time (ISO). e.g., 2024-10-04T12:00:00Z")
    args = ap.parse_args()

    # 기준 시각(now_utc)
    if args.now:
        ref = args.now.replace(" ", "T")
        if ref.endswith("Z"): ref = ref.replace("Z", "+00:00")
        now_utc = datetime.fromisoformat(ref)
        if now_utc.tzinfo is None: now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = utc_now()

    yesterday = (now_utc.date() - timedelta(days=1))

    start_d = parse_utc_date(args.start)
    if start_d is None:
        raise SystemExit("--start must be a date")

    # end 처리: None or 'yesterday' → 어제(UTC)
    if args.end is None or (args.end and args.end.strip().lower() == "yesterday"):
        end_d = yesterday
    else:
        end_parsed = parse_utc_date(args.end)
        if end_parsed is None:
            end_d = yesterday
        else:
            end_d = end_parsed

    # 기본: 어제(UTC)로 캡
    if not args.allow_today and end_d > yesterday:
        end_d = yesterday

    if end_d < start_d:
        raise SystemExit(f"--end becomes {end_d} (after yesterday-cap); must be >= --start ({start_d}).")

    # 프리뷰 출력
    print(f"[INFO] reference_now_utc = {now_utc.isoformat()}")
    print(f"[INFO] yesterday_utc     = {yesterday.isoformat()}")
    print(f"[INFO] effective range   = {start_d} .. {end_d} (inclusive)")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    cur = start_d
    while cur <= end_d:
        for sym in symbols:
            try:
                ingest_one_day(
                    symbol=sym,
                    interval=args.interval,
                    d=cur,
                    out_root=args.out,
                    limit=args.limit,
                    target_weight_per_minute=args.weight,
                    force=args.force
                )
            except KeyboardInterrupt:
                print("\nInterrupted."); sys.exit(1)
            except Exception as e:
                print(f"[{sym}] ERROR {cur}: {e}", file=sys.stderr)
        cur += timedelta(days=1)

if __name__ == "__main__":
    main()
