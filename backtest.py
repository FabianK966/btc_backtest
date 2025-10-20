import pandas as pd
import requests
import time
import mysql.connector
from datetime import datetime
import uuid

# --- Logik-Funktionen ---
def fetch_ohlcv(symbol="BTCUSDT", intervall="60", category="spot", days=730, progress_callback=None):
    base_url = "https://api.bybit.com/v5/market/kline"
    all_data = []
    limit = 1000
    ms_per_candle = 60 * 60 * 1000
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - days * 24 * ms_per_candle
    total_candles = days * 24
    fetched = 0

    while True:
        params = {
            "category": category,
            "symbol": symbol,
            "interval": intervall,
            "start": start_ms,
            "end": now_ms,
            "limit": limit
        }
        resp = requests.get(base_url, params=params)
        data = resp.json()["result"]["list"]
        if not data:
            break

        all_data.extend(data)
        oldest_ts = int(data[-1][0])
        now_ms = oldest_ts - 1
        fetched += len(data)

        if progress_callback:
            progress_callback(min(100, fetched / total_candles * 100))

        if now_ms <= start_ms:
            break

        time.sleep(0.2)

    df = pd.DataFrame(all_data, columns=["timestamp","open","high","low","close","volume","turnover"])
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors='coerce')
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
    df.set_index("timestamp", inplace=True)
    df.sort_index(inplace=True)
    return df

def run_backtest(df, session_start="15:30:00", session_end="22:00:00"):
    daily = df.resample("1D").agg({"high":"max","low":"min"})
    count_hits = 0
    count_days = 0
    records = []

    for date in daily.index[1:]:
        prev_high = daily.loc[date - pd.Timedelta(days=1), "high"]
        prev_low = daily.loc[date - pd.Timedelta(days=1), "low"]
        mask = (df.index.date == date.date()) & \
               (df.index.time >= pd.to_datetime(session_start).time()) & \
               (df.index.time <= pd.to_datetime(session_end).time())
        session_data = df.loc[mask]
        if not session_data.empty:
            count_days += 1
            day_high = session_data["high"].max()
            day_low = session_data["low"].min()
            session_close = session_data["close"].iloc[-1]  # Schlusskurs der letzten Kerze
            touched_high = day_high >= prev_high and session_close >= prev_high
            touched_low = day_low <= prev_low and session_close <= prev_low
            if touched_high or touched_low:
                count_hits += 1
            records.append({
                "Date": date.date(),
                "Touched High": touched_high,
                "High Price": day_high,
                "Touched Low": touched_low,
                "Low Price": day_low,
                "Close Price": session_close
            })

    return {
        "count_days": count_days,
        "count_hits": count_hits,
        "records": records,
        "daily": daily
    }

def save_results_to_db(records, symbol, intervall, category, session_start, session_end, result_stats):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="btc_backtest"
    )
    cursor = conn.cursor()

    backtest_id = str(uuid.uuid4())

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS backtest_metadata (
        backtest_id VARCHAR(36) PRIMARY KEY,
        symbol VARCHAR(20),
        intervall VARCHAR(10),
        category VARCHAR(20),
        session_start VARCHAR(10),
        session_end VARCHAR(10),
        total_days INT,
        total_hits INT,
        hit_ratio DECIMAL(5,2),
        start_date DATE,
        end_date DATE,
        created_at DATETIME
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS backtest_daily_results (
        id INT AUTO_INCREMENT PRIMARY KEY,
        backtest_id VARCHAR(36),
        date DATE,
        touched_high BOOLEAN,
        high_price DOUBLE,
        touched_low BOOLEAN,
        low_price DOUBLE,
        close_price DOUBLE,
        FOREIGN KEY (backtest_id) REFERENCES backtest_metadata(backtest_id)
    )
    """)

    cursor.execute("""
    INSERT INTO backtest_metadata 
    (backtest_id, symbol, intervall, category, session_start, session_end, 
     total_days, total_hits, hit_ratio, start_date, end_date, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        backtest_id,
        symbol,
        intervall,
        category,
        session_start,
        session_end,
        result_stats["count_days"],
        result_stats["count_hits"],
        result_stats["count_hits"] / result_stats["count_days"] * 100 if result_stats["count_days"] > 0 else 0,
        result_stats["daily"].index[0].date(),
        result_stats["daily"].index[-1].date(),
        datetime.now()
    ))

    for r in records:
        cursor.execute("""
        INSERT INTO backtest_daily_results 
        (backtest_id, date, touched_high, high_price, touched_low, low_price, close_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            backtest_id,
            r["Date"],
            bool(r["Touched High"]),
            float(r["High Price"]),
            bool(r["Touched Low"]),
            float(r["Low Price"]),
            float(r["Close Price"])
        ))

    conn.commit()
    cursor.close()
    conn.close()
    
    return backtest_id