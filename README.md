quant-pipeline – README (Commands first)

개요
- Binance 현물 1초봉(1s) OHLCV 수집 → 보조지표 생성 → 검증까지 일괄 처리.
- 모든 스크립트는 폴더가 없어도 자동 생성합니다. 수동으로 data/…를 만들 필요 없음.
- PowerShell에서는 여러 줄 명령을 백틱 ` 로, CMD에서는 ^ 를 사용하거나 한 줄로 입력하세요.

------------------------------------------------------------
0) 설치
------------------------------------------------------------
python -m pip install -r requirements.txt

(참고) data/ 아래는 .gitignore 로 커밋 제외됩니다.

------------------------------------------------------------
1) 1초봉 OHLCV 수집
------------------------------------------------------------

1-1) 개별 호출: scripts/01_fetch_ohlcv.py
- 규칙:
  * --end 를 생략하면 어제(UTC)까지 자동 캡.
  * --end 를 주더라도 어제(UTC)보다 크면 어제(UTC)로 캡.
- 출력 경로: data/ohlcv/binance-spot/1s/{SYMBOL}/{YYYY-MM-DD}.parquet

예시(어제까지):
PowerShell:
  python scripts/01_fetch_ohlcv.py `
    --symbols BTCUSDT,ETHUSDT `
    --interval 1s `
    --start 2024-10-01 `
    --out data\ohlcv\binance-spot

CMD:
  python scripts\01_fetch_ohlcv.py ^
    --symbols BTCUSDT,ETHUSDT ^
    --interval 1s ^
    --start 2024-10-01 ^
    --out data\ohlcv\binance-spot

예시(범위 지정, 그래도 어제 캡 적용):
PowerShell:
  python scripts/01_fetch_ohlcv.py `
    --symbols BTCUSDT `
    --interval 1s `
    --start 2024-10-01 `
    --end 2024-10-10 `
    --out data\ohlcv\binance-spot

주요 옵션:
  --symbols   콤마 구분(예: BTCUSDT,ETHUSDT)
  --interval  1s 권장(구조상 다른 간격도 대응)
  --start/--end  YYYY-MM-DD (inclusive)
  --out       출력 루트(기본: data/ohlcv/binance-spot)
  --limit     klines page size (<=1000, 기본 1000)
  --weight    분당 used-weight 목표(기본 5000). 헤더 X-MBX-USED-WEIGHT-1M 감시해 자동 대기
  --force     파일이 있어도 덮어쓰기
  --allow-today  어제 캡을 해제(실시간 수집)
  --now       기준 시간 고정(재현 목적), 예: 2024-10-04T12:00:00Z

1-2) 즐겨찾기 묶음 수집: scripts/01_2_fetch_favorites.py
- 기본 목록: btc, eth, xrp, sol, bnb, doge, trx, ada, link, avax, xlm, bch, ltc, dot (모두 USDT 페어)
- 기간: 2023-01-01 ~ 어제(UTC)

실행:
  python scripts/01_2_fetch_favorites.py

(원하면 스크립트 내부 FAVORITES, start_date 수정)

------------------------------------------------------------
2) 보조지표 생성
------------------------------------------------------------

입력:  data/ohlcv/binance-spot/1s/{SYMBOL}/{YYYY-MM-DD}.parquet
출력:  data/features-all/binance-spot/1s/{SYMBOL}/{YYYY-MM-DD}.parquet
특징:
- 워밍업 자동: 이전 날짜 파일에서 최대 창 길이+버퍼만큼 앞 구간을 이어붙여 계산 후, 당일만 잘라 저장 → 초반 NaN 최소화
- --with-custom: 바이낸스 전용 커스텀 피처(주문플로/실현변동성 등) 포함

2-1) 개별 호출: scripts/02_make_features_all.py

예시:
PowerShell:
  python scripts/02_make_features_all.py `
    --symbols BTCUSDT `
    --start 2024-10-01 `
    --end 2024-10-10 `
    --with-custom

CMD:
  python scripts\02_make_features_all.py ^
    --symbols BTCUSDT ^
    --start 2024-10-01 ^
    --end 2024-10-10 ^
    --with-custom

옵션:
  --symbols      콤마 구분(미지정 시 입력 디렉토리 자동 탐색)
  --start/--end  처리 날짜 범위
  --with-custom  커스텀 피처 추가
  --force        결과 덮어쓰기
  --warmup N     워밍업 행 수 수동 지정(미지정 시 자동)

2-2) 즐겨찾기 묶음 생성: scripts/02_3_make_features_favorites.py
- 코인 묶음(위 즐겨찾기) + 2023-02-01 ~ 2025-09-30 고정
- 기본: --with-custom 포함, 기존 파일 있으면 스킵

실행(기본):
  python scripts/02_3_make_features_favorites.py

옵션:
  --no-custom  커스텀 피처 제외
  --force      덮어쓰기
  --warmup N   워밍업 수동 지정(미지정 시 자동)

------------------------------------------------------------
3) 피처 검증(무결성/NaN 비율) – 선택
------------------------------------------------------------

scripts/02_2_validate_features.py

예시(단일 파일 검증):
  python scripts/02_2_validate_features.py data\features-all\binance-spot\1s\BTCUSDT\2024-10-01.parquet

예시(여러 파일 연속 검증):
  python scripts/02_2_validate_features.py ^
    data\features-all\binance-spot\1s\BTCUSDT\2024-10-01.parquet ^
    data\features-all\binance-spot\1s\BTCUSDT\2024-10-02.parquet

출력 내용:
- shape, open_time 간격/연속성(1s 비율), NULL 비율 상위 컬럼, 대표 지표의 not-null 비율 등

------------------------------------------------------------
폴더 구조(요약)
------------------------------------------------------------
data/
  ohlcv/
    binance-spot/
      1s/
        BTCUSDT/
          2024-10-01.parquet
          2024-10-02.parquet
        ETHUSDT/
          ...
  features-all/
    binance-spot/
      1s/
        BTCUSDT/
          2024-10-01.parquet
          2024-10-02.parquet
        ETHUSDT/
          ...

------------------------------------------------------------
자주 묻는 질문(FAQ)
------------------------------------------------------------
Q1) 왜 어제(UTC)까지만 저장하나요?
A) 실시간 봉은 마감되지 않아 일부 지표(특히 상태형, 길이 긴 지표)에서 과도한 NaN/불안정이 생길 수 있습니다. 기본 동작은 어제까지 캡이며, --allow-today 로 해제할 수 있습니다.

Q2) 일부 지표에서 앞부분 NaN이 보입니다.
A) 길이가 긴 지표는 워밍업이 충분해도 NaN이 존재할 수 있습니다(예: QQE, PSAR, Supertrend 등 상태형). 일반적인 이동평균 기반 지표는 워밍업 덕분에 초반 NaN이 크게 줄어듭니다.

Q3) Rate Limit(429)이 나면?
A) 헤더 X-MBX-USED-WEIGHT-1M 을 모니터링하고, target(--weight) 근처면 자동으로 sleep 합니다. 429/418 응답 시 백오프(최대 30초)로 재시도합니다.

Q4) 폴더가 없어 에러가 나나요?
A) 스크립트가 모든 출력 경로를 자동 생성합니다. 수동 생성은 필요 없습니다.

끝.
