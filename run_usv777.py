import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import warnings
import math

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbzDWTkTPXZof6GPbJ8ylmckDRPzWDtlLtB_9sMRsEhtyW0Hmhr833oZLdMbuPmw0XRy/exec"

# 核心壟斷巨頭股票池
MONOPOLY_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "TSM", "ASML", "AVGO",
    "V", "MA", "JPM", "BRK-B", "SPGI", "MCO", 
    "LLY", "NVO", "UNH", "JNJ", "ISRG",       
    "WMT", "COST", "PG", "KO", "PEP",         
    "LIN", "SHW", "CAT", "DE", "LMT",         
    "UNP", "WM", "RSG", "NOW", "SNPS", "CDNS"
]

# 策略參數
MAX_DRAWDOWN = -0.30  # 跌幅要求
MAX_RSI = 30.0        # 週 RSI 要求

# ==========================================
# 2. 技術指標與基礎計算
# ==========================================
def calculate_weekly_rsi(prices, period=14):
    """計算 Wilder's RSI (處理了 NaN 與無限大防護)"""
    if len(prices) < period + 1:
        return 50.0
    delta = prices.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=period-1, adjust=False).mean()
    ema_down = down.ewm(com=period-1, adjust=False).mean()
    rs = ema_up / ema_down
    rsi = 100 - (100 / (1 + rs))
    
    final_rsi = float(rsi.iloc[-1])
    return final_rsi if math.isfinite(final_rsi) else 50.0

# ==========================================
# 3. 极速基本面防错与快取机制
# ==========================================
def check_fundamentals(ticker):
    """
    極速版：優先驗證 TTM 數據。只要營收>1億且 FCF與ROE為正即過關。
    大幅減少請求 .financials 導致的卡頓與 IP 封鎖。
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # 1. 營收檢查 (Revenue > $100M)
        revenue = info.get('totalRevenue', 0)
        if revenue is not None and revenue < 100_000_000:
            return False, f"營收不足: ${revenue/1e6:.1f}M"

        # 2. 首選 TTM 數據快速通關
        fcf_ttm = info.get('freeCashflow', 0)
        roe_ttm = info.get('returnOnEquity', 0)
        
        # 處理 None 值
        fcf_ttm = fcf_ttm if fcf_ttm is not None else 0
        roe_ttm = roe_ttm if roe_ttm is not None else 0

        if fcf_ttm > 0 and roe_ttm > 0:
            return True, f"TTM 健康 (FCF: ${fcf_ttm/1e9:.1f}B)"

        # 3. 如果 info 抓不到，才迫不得已去抓財報 (容錯機制)
        cashflow = stock.cashflow
        if not cashflow.empty and 'Free Cash Flow' in cashflow.index:
            recent_fcf = cashflow.loc['Free Cash Flow'].iloc[0]
            if recent_fcf > 0:
                return True, "財報 FCF 為正"
                
        return False, "現金流或回報不達標"

    except Exception as e:
        return False, "API 抓取超時或無數據"

# ==========================================
# 4. 核心篩選引擎 (降維打擊版)
# ==========================================
def run_quality_dip_scanner():
    start_time = time.time()
    print("💎 [高品質-黃金坑期權策略 V2] 啟動...")
    
    try:
        # 【優化】period="max" 確保抓到真正的歷史最高點，避免把半山腰當作高點
        print(f"📡 正在下載 {len(MONOPOLY_TICKERS)} 隻標的的歷史復權數據...")
        data_daily = yf.download(MONOPOLY_TICKERS, period="max", interval="1d", group_by='ticker', auto_adjust=True, threads=True, progress=False)
    except Exception as e:
        print(f"❌ 數據下載失敗: {e}"); return

    candidates = []

    for t in MONOPOLY_TICKERS:
        try:
            # 【優化】安全提取單隻股票數據，兼容不同版本的 pandas/yfinance
            if isinstance(data_daily.columns, pd.MultiIndex):
                df_d = data_daily[t].dropna()
            else:
                df_d = data_daily.dropna() if len(MONOPOLY_TICKERS) == 1 else pd.DataFrame()
                
            if len(df_d) < 200: continue # 數據過短跳過

            close_d = df_d['Close']
            vol_d = df_d['Volume']
            curr_price = float(close_d.iloc[-1])

            # 降採樣: 日線 -> 週線
            df_w = df_d.resample('W-FRI').agg({
                'High': 'max',
                'Low': 'min',
                'Close': 'last'
            }).dropna()

            # 1. 高點跌幅計算 (Drawdown < -30%)
            ath = float(df_w['High'].max())
            drawdown = (curr_price - ath) / ath
            if drawdown > MAX_DRAWDOWN: continue

            # 2. 週線 RSI(14) < 30
            weekly_rsi = calculate_weekly_rsi(df_w['Close'], 14)
            if weekly_rsi >= MAX_RSI: continue

            # 3. 60天平均交易額 > 1億美元
            trade_value_60d = (close_d.tail(60) * vol_d.tail(60)).mean()
            if trade_value_60d < 100_000_000: continue

            # 4. 200週均線距離
            wma_200 = float(df_w['Close'].tail(200).mean()) if len(df_w) >= 200 else float(df_w['Close'].mean())
            dist_200wma = (curr_price - wma_200) / wma_200

            print(f"🔎 發現黃金坑候選: [{t}] 跌幅 {drawdown*100:.1f}%，進入基本面審查...")

            # 5. 執行基本面審查 (只對技術面通關的股票執行，極大節約時間)
            passed, fund_msg = check_fundamentals(t)
            
            if passed:
                target_strike = curr_price * 1.10
                target_dte_date = (datetime.datetime.now() + datetime.timedelta(days=365)).strftime('%Y-%m-%d')
                
                candidates.append({
                    "Ticker": t, "Price": curr_price, "ATH_Drawdown": drawdown * 100,
                    "Weekly_RSI": weekly_rsi, "Trade_Value_M": trade_value_60d / 1_000_000,
                    "Fund_Status": fund_msg, "Distance_200WMA": dist_200wma * 100,
                    "Target_Strike": target_strike, "Target_DTE": f"> {target_dte_date}"
                })
        except Exception as e:
            continue

    # ==========================================
    # 5. 輸出與同步處理
    # ==========================================
    if not candidates:
        print("📭 當前市場無符合「高品質+深跌超賣」的標的。（符合策略設計）")
        candidates = [{"Ticker": "無符合標的", "Price": 0, "ATH_Drawdown": 0, "Weekly_RSI": 0, 
                       "Trade_Value_M": 0, "Fund_Status": "等待市場錯殺機會", 
                       "Distance_200WMA": 0, "Target_Strike": 0, "Target_DTE": "-"}]

    sorted_df = pd.DataFrame(candidates).sort_values(by="ATH_Drawdown", ascending=True)
    final_list = []
    
    for _, row in sorted_df.reset_index().iterrows():
        if str(row['Ticker']) == "無符合標的":
            final_list.append(["無符合標的", "-", "-", "-", "-", "-", "-", "-", "-", "-"])
            break
            
        final_list.append([
            str(row['Ticker']),
            round(float(row['Price']), 2),
            f"{round(float(row['ATH_Drawdown']), 2)}%",
            round(float(row['Weekly_RSI']), 2),
            f"${round(float(row['Trade_Value_M']), 0)}M",
            str(row['Fund_Status']),
            f"{round(float(row['Distance_200WMA']), 2)}%",
            f"Call @ ${round(float(row['Target_Strike']), 2)}",
            str(row['Target_DTE']),
            "翻倍平半 / 60天平倉 / 零止損"
        ])

    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    header = [
        ["💎 高品質左側抄底期權策略", "更新:", bj_now, "策略屬性:", "極端錯殺/勝率極高", "", "", "", "", ""],
        ["代碼", "現價", "距歷史高點跌幅", "週RSI(14)", "60天均交易額", "基本面狀態", "距200週線", "建議行權價(10% OTM)", "建議到期日(DTE)", "交易紀律"]
    ]
    
    matrix = header + final_list
    
    try:
        # 強制替換 NaN 為 0，防止 JSON 崩潰
        def safe_json_val(val):
            if isinstance(val, float) and not math.isfinite(val): return 0
            return str(val)
            
        payload = json.loads(json.dumps(matrix, default=safe_json_val))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 雲端同步完成！總耗時: {round(time.time() - start_time, 2)}秒")
    except Exception as e:
        print(f"❌ 雲端同步失敗: {e}")

if __name__ == "__main__":
    run_quality_dip_scanner()
