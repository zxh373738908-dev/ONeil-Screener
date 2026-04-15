import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbzDWTkTPXZof6GPbJ8ylmckDRPzWDtlLtB_9sMRsEhtyW0Hmhr833oZLdMbuPmw0XRy/exec"

# 核心壟斷巨頭股票池
MONOPOLY_TICKERS =[
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "TSM", "ASML", "AVGO",
    "V", "MA", "JPM", "BRK-B", "SPGI", "MCO", 
    "LLY", "NVO", "UNH", "JNJ", "ISRG",       
    "WMT", "COST", "PG", "KO", "PEP",         
    "LIN", "SHW", "CAT", "DE", "LMT",         
    "UNP", "WM", "RSG", "NOW", "SNPS", "CDNS"
]

# ==========================================
# 2. 技術指標與基礎計算
# ==========================================
def calculate_weekly_rsi(prices, period=14):
    """計算 Wilder's RSI"""
    if len(prices) < period + 1:
        return 50.0
    delta = prices.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ema_up = up.ewm(com=period-1, adjust=False).mean()
    ema_down = down.ewm(com=period-1, adjust=False).mean()
    rs = ema_up / ema_down
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])

# ==========================================
# 3. 基本面防錯與快取機制
# ==========================================
def check_fundamentals(ticker):
    """
    優化版基本面檢查：優先使用 .info 的 TTM 數據防超時，再嘗試獲取歷史財務報表
    """
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # 1. 營收檢查 (Revenue > $100M)
        revenue = info.get('totalRevenue', 0)
        if revenue and revenue < 100_000_000:
            return False, f"營收不足: ${revenue/1e6:.1f}M"

        # 2. 獲取 TTM 數據作為首選防禦
        fcf_ttm = info.get('freeCashflow', 0)
        roe_ttm = info.get('returnOnEquity', 0)
        op_margin = info.get('operatingMargins', 0)

        # 嘗試獲取詳細報表 (設置重試與容錯)
        financials = stock.financials
        cashflow = stock.cashflow
        
        if not financials.empty and not cashflow.empty:
            fcf_margins, roics = [],[]
            for date in financials.columns[:5]:
                try:
                    rev = financials.loc['Total Revenue', date] if 'Total Revenue' in financials.index else 0
                    fcf = cashflow.loc['Free Cash Flow', date] if 'Free Cash Flow' in cashflow.index else 0
                    net_inc = financials.loc['Net Income', date] if 'Net Income' in financials.index else 0
                    
                    if rev > 0: fcf_margins.append(fcf / rev)
                    if rev > 0: roics.append(net_inc) # 以淨利為正替代ROIC為正
                except: continue
            
            avg_fcf_margin = np.mean(fcf_margins) if fcf_margins else op_margin
            avg_profit = np.mean(roics) if roics else roe_ttm
            
            if avg_fcf_margin > 0 and avg_profit > 0:
                return True, f"FCF率: {avg_fcf_margin*100:.1f}% | 盈利健康"
            else:
                return False, f"現金流/回報不達標"

        # 如果財報抓取失敗，依賴 TTM 數據
        if fcf_ttm > 0 and roe_ttm > 0:
            return True, f"TTM狀態良好 (FCF: ${fcf_ttm/1e9:.1f}B)"
        
        return False, "基本面數據不足以支撐"

    except Exception as e:
        return False, "API拒絕/超時"

# ==========================================
# 4. 核心篩選引擎 (降維打擊版)
# ==========================================
def run_quality_dip_scanner():
    start_time = time.time()
    print("💎[高品質-黃金坑期權策略 V2] 啟動...")
    
    try:
        # 【優化1】只下載一次 5 年日線數據，並開啟 auto_adjust=True 處理拆股復權
        print("📡 正在下載歷史復權數據 (避免拆股導致的假暴跌)...")
        data_daily = yf.download(MONOPOLY_TICKERS, period="5y", interval="1d", group_by='ticker', auto_adjust=True, threads=True, progress=False)
    except Exception as e:
        print(f"❌ 數據下載失敗: {e}"); return

    candidates =[]

    for t in MONOPOLY_TICKERS:
        try:
            # 安全提取單隻股票數據
            df_d = data_daily[t].dropna() if len(MONOPOLY_TICKERS) > 1 else data_daily.dropna()
            if len(df_d) < 200: continue # 數據過短跳過

            close_d = df_d['Close']
            vol_d = df_d['Volume']
            curr_price = float(close_d.iloc[-1])

            # 【優化2】降採樣 (Resampling): 將日線轉換為週線，避免二次發送 API 請求
            df_w = df_d.resample('W-FRI').agg({
                'High': 'max',
                'Low': 'min',
                'Close': 'last'
            }).dropna()

            # 1. 高點跌幅計算 (Drawdown < -30%)
            ath = float(df_w['High'].max())
            drawdown = (curr_price - ath) / ath
            if drawdown > -0.30: continue

            # 2. 週線 RSI(14) < 30
            weekly_rsi = calculate_weekly_rsi(df_w['Close'], 14)
            if weekly_rsi >= 30: continue

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
                # 精確計算 360 天後的日期
                target_dte_date = (datetime.datetime.now() + datetime.timedelta(days=365)).strftime('%Y-%m-%d')
                
                candidates.append({
                    "Ticker": t, "Price": curr_price, "ATH_Drawdown": drawdown * 100,
                    "Weekly_RSI": weekly_rsi, "Trade_Value_M": trade_value_60d / 1_000_000,
                    "Fund_Status": fund_msg, "Distance_200WMA": dist_200wma * 100,
                    "Target_Strike": target_strike, "Target_DTE": f"> {target_dte_date}"
                })
        except Exception as e:
            continue

    if not candidates:
        print("📭 當前市場無符合「高品質+深跌超賣」的標的。（符合策略設計）")
        candidates =[{"Ticker": "無符合標的", "Price": 0, "ATH_Drawdown": 0, "Weekly_RSI": 0, 
                       "Trade_Value_M": 0, "Fund_Status": "等待市場錯殺機會", 
                       "Distance_200WMA": 0, "Target_Strike": 0, "Target_DTE": "-"}]

    sorted_df = pd.DataFrame(candidates).sort_values(by="ATH_Drawdown", ascending=True)
    final_list =[]
    
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
    header = [["💎 高品質左側抄底期權策略", "更新:", bj_now, "策略屬性:", "極端錯殺/勝率極高", "", "", "", "", ""],["代碼", "現價", "距歷史高點跌幅", "週RSI(14)", "60天均交易額", "基本面狀態", "距200週線", "建議行權價(10% OTM)", "建議到期日(DTE)", "交易紀律"]
    ]
    
    matrix = header + final_list
    
    try:
        payload = json.loads(json.dumps(matrix, default=lambda x: str(x)))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 雲端同步完成！總耗時: {round(time.time() - start_time, 2)}秒")
    except Exception as e:
        print(f"❌ 雲端同步失敗: {e}")

if __name__ == "__main__":
    run_quality_dip_scanner()
