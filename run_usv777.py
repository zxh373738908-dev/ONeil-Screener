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
# 1. 系統配置中心
# ==========================================
# 替換為你最新提供的專屬 Webhook URL
WEBAPP_URL = "https://script.google.com/macros/s/AKfycby1pIM7iO43lcLQpOmi5LCJIn3VN9a0Ilf9amoy1EtQV_GBXJkk_A4PpsrJxKzH7i51/exec"

# 左側策略：嚴格的壟斷巨頭名單
MONOPOLY_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "TSM", "ASML", "AVGO",
    "V", "MA", "JPM", "BRK-B", "SPGI", "MCO", "LLY", "NVO", "UNH", "JNJ", "ISRG",       
    "WMT", "COST", "PG", "KO", "PEP", "LIN", "SHW", "CAT", "DE", "LMT",         
    "UNP", "WM", "RSG", "NOW", "SNPS", "CDNS"
]

# 右側策略：如果維基百科抓取失敗，使用的超強備用池 (納斯達克核心+標普代表)
FALLBACK_UNIVERSE = MONOPOLY_TICKERS + [
    "AMD", "CRWD", "PLTR", "PANW", "SNOW", "DDOG", "NET", "MDB", "TEAM", "WDAY",
    "ADBE", "CRM", "INTU", "QCOM", "TXN", "AMAT", "LRCX", "KLAC", "MU", "ARM",
    "UBER", "ABNB", "BKNG", "NFLX", "DIS", "CMCSA", "TMUS", "VZ", "T",
    "JPM", "BAC", "WFC", "GS", "MS", "AXP", "BLK", "C", "PGR", "CB",
    "XOM", "CVX", "COP", "EOG", "SLB", "GE", "RTX", "BA", "HON", "UPS"
]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'Credit Services']

# ==========================================
# 2. 通用工具與 Google 雲端同步函數
# ==========================================
def sync_to_google_sheet(sheet_name, matrix):
    """將二維陣列數據同步到指定的 Google Sheet 分頁"""
    try:
        # 強制清理 NaN 或 Infinity，防止 JSON 報錯
        def safe_json_val(val):
            if isinstance(val, float) and not math.isfinite(val): return 0
            return str(val)
            
        payload = {
            "sheet_name": sheet_name,
            "data": json.loads(json.dumps(matrix, default=safe_json_val))
        }
        resp = requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 同步成功 -> 分頁: [{sheet_name}]")
    except Exception as e:
        print(f"❌ 同步失敗 [{sheet_name}]: {e}")

def calculate_weekly_rsi(prices, period=14):
    if len(prices) < period + 1: return 50.0
    delta = prices.diff()
    up = delta.clip(lower=0).ewm(com=period-1, adjust=False).mean()
    down = (-1 * delta.clip(upper=0)).ewm(com=period-1, adjust=False).mean()
    rs = up / down
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])

# ==========================================
# 3. 🛡️ 策略 A: 左側黃金坑 (Quality Deep Dip)
# ==========================================
def run_left_side_golden_pit():
    print("\n" + "="*50)
    print("🛡️ [策略 A: 左側黃金坑] 啟動掃描...")
    start_time = time.time()
    
    try:
        data = yf.download(MONOPOLY_TICKERS, period="max", interval="1d", group_by='ticker', auto_adjust=True, progress=False)
    except:
        print("❌ 數據下載失敗"); return

    candidates = []
    for t in MONOPOLY_TICKERS:
        try:
            df = data[t].dropna() if isinstance(data.columns, pd.MultiIndex) else data.dropna()
            if len(df) < 250: continue
            
            close, vol = df['Close'], df['Volume']
            curr_price = float(close.iloc[-1])
            df_w = df.resample('W-FRI').agg({'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()
            
            # 技術面漏斗
            ath = float(df_w['High'].max())
            drawdown = (curr_price - ath) / ath
            if drawdown > -0.30: continue
            
            weekly_rsi = calculate_weekly_rsi(df_w['Close'], 14)
            if weekly_rsi >= 30: continue
            
            if (close.tail(60) * vol.tail(60)).mean() < 100_000_000: continue
            
            wma_200 = float(df_w['Close'].tail(200).mean())
            dist_200 = (curr_price - wma_200) / wma_200

            # 基本面防價值陷阱
            info = yf.Ticker(t).info
            fcf = info.get('freeCashflow', 0)
            roe = info.get('returnOnEquity', 0)
            rev_growth = info.get('revenueGrowth', 0) # 【改進1】防護網
            
            if fcf > 0 and roe > 0 and (rev_growth is None or rev_growth > -0.10):
                target_strike_itm = curr_price * 0.80 # Deep ITM
                candidates.append({
                    "Ticker": t, "Price": curr_price, "Drawdown": drawdown*100,
                    "RSI": weekly_rsi, "Status": f"FCF健康 (增長:{rev_growth*100:.1f}%)", 
                    "Strike": target_strike_itm
                })
        except: continue

    # 輸出整理
    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    header = [
        ["🛡️ 左側黃金坑 (價值回歸)", "更新:", bj_now, "策略:", "極端錯殺/Deep ITM期權", "", "", ""],
        ["代碼", "現價", "距歷史高點跌幅", "週RSI(14)", "基本面狀態", "建議買入合約", "建議到期日", "交易紀律"]
    ]
    
    final_list = []
    if not candidates:
        final_list.append(["-", "等待股災錯殺", "-", "-", "-", "-", "-", "-"])
    else:
        df_cand = pd.DataFrame(candidates).sort_values(by="Drawdown", ascending=True)
        for _, row in df_cand.iterrows():
            final_list.append([
                row['Ticker'], round(row['Price'], 2), f"{round(row['Drawdown'], 2)}%",
                round(row['RSI'], 2), row['Status'], f"Deep ITM Call @ ${round(row['Strike'], 2)}",
                "> 360 Days (LEAPS)", "翻倍平半 / 剩120天平倉 / 零止損"
            ])
            
    sync_to_google_sheet("🛡️左側_黃金坑", header + final_list)

# ==========================================
# 4. 🚀 策略 B: 右側動能成長 (CAN SLIM)
# ==========================================
def run_right_side_momentum():
    print("\n" + "="*50)
    print("🚀 [策略 B: 右側動能成長] 啟動掃描...")
    start_time = time.time()
    
    # 大盤濾網
    try:
        spy = yf.download("SPY", period="6mo", progress=False)['Close']
        if isinstance(spy, pd.DataFrame): spy = spy.iloc[:, 0]
        curr_spy, ma50_spy = float(spy.iloc[-1]), float(spy.tail(50).mean())
        is_bull = curr_spy > ma50_spy
    except:
        is_bull = True

    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    header = [
        ["🚀 動能成長 Top 10", "更新:", bj_now, "大盤狀態:", "🟢 多頭允許交易" if is_bull else "🔴 轉弱暫停交易", "", "", "", "", ""],
        ["排名", "代碼", "板塊", "現價", "近1月漲幅", "近3月漲幅", "RS動能分", "基本面護城河", "綜合總分", "交易紀律"]
    ]

    if not is_bull:
        print("🛑 大盤跌破50MA，停止右側交易。")
        sync_to_google_sheet("🚀右側_動能成長", header + [["-", "大盤轉弱", "-", "-", "-", "-", "-", "保留現金", "-", "觀望 / 嚴禁追高"]])
        return

    # 【改進3】防護型股票池獲取
    try:
        tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        tickers = [t.replace('.', '-') for t in tables[0]['Symbol'].tolist()]
    except:
        tickers = FALLBACK_UNIVERSE
        
    print(f"📥 實際掃描股票池數量: {len(tickers)} 隻")

    try:
        data = yf.download(tickers, period="1y", interval="1d", group_by='ticker', auto_adjust=True, threads=True, progress=False)
    except: return

    cands = []
    for t in tickers:
        try:
            df = data[t].dropna() if isinstance(data.columns, pd.MultiIndex) else data.dropna()
            if len(df) < 200: continue
            
            close, vol = df['Close'], df['Volume']
            curr_price = float(close.iloc[-1])
            
            # 【改進2】均線多頭 (放寬20MA) + 成交量確認
            ma50, ma200 = close.tail(50).mean(), close.tail(200).mean()
            vol_recent, vol_ma10 = vol.iloc[-1], vol.tail(10).mean()
            
            if curr_price < ma50 or ma50 < ma200: continue
            if vol_ma10 < 500_000: continue
            
            # RS 動能計算
            r1m = (curr_price - close.iloc[-21]) / close.iloc[-21] if len(close)>=21 else 0
            r3m = (curr_price - close.iloc[-63]) / close.iloc[-63] if len(close)>=63 else 0
            r1y = (curr_price - close.iloc[-252]) / close.iloc[-252] if len(close)>=252 else 0
            
            rs_score = (r1m * 0.4) + (r3m * 0.3) + (r1y * 0.3)
            if rs_score < 0: continue
            
            cands.append({"Ticker": t, "Price": curr_price, "RS": rs_score, "1M": r1m, "3M": r3m, "Vol_OK": vol_recent > vol_ma10})
        except: continue

    # 取 RS 前 40 名測基本面
    cands = sorted(cands, key=lambda x: x['RS'], reverse=True)[:40]
    final_cands = []
    
    for c in cands:
        try:
            info = yf.Ticker(c['Ticker']).info
            sec, ind = info.get('sector', ''), info.get('industry', '')
            if any(ex.lower() in sec.lower() or ex.lower() in ind.lower() for ex in EXCLUDED_INDUSTRIES): continue
            
            # Rule of 40 or Z-Score 簡化版
            fin_score, fin_msg = 0, ""
            if 'Technology' in sec or 'Software' in ind:
                r40 = (info.get('revenueGrowth', 0) + (info.get('freeCashflow', 0)/(info.get('totalRevenue', 1) or 1))) * 100
                if r40 >= 40:
                    fin_score, fin_msg = r40, f"Rule of 40: {r40:.1f}%"
            else:
                mkt_cap, tl = info.get('marketCap', 1), info.get('totalDebt', 1) or 1
                z = (mkt_cap / tl) # 極度簡化的財務安全係數
                if z > 1.5:
                    fin_score, fin_msg = z*10, f"債務健康度高"

            if fin_score > 0:
                final_cands.append({
                    "T": c['Ticker'], "Sec": sec, "P": c['Price'], "1M": c['1M']*100, 
                    "3M": c['3M']*100, "RS": c['RS']*100, "Msg": fin_msg + (" (放量)" if c['Vol_OK'] else ""),
                    "Tot": (c['RS']*100) + fin_score
                })
        except: continue

    top10 = sorted(final_cands, key=lambda x: x['Tot'], reverse=True)[:10]
    final_list = []
    
    if not top10:
        final_list.append(["-", "無符合標的", "-", "-", "-", "-", "-", "-", "-", "-"])
    else:
        for i, r in enumerate(top10):
            final_list.append([
                f"Top {i+1}", r['T'], r['Sec'], round(r['P'], 2), f"{round(r['1M'], 1)}%", 
                f"{round(r['3M'], 1)}%", round(r['RS'], 2), r['Msg'], round(r['Tot'], 2),
                "回踩 20EMA 買入 / 破 50MA 止損"
            ])
            
    sync_to_google_sheet("🚀右側_動能成長", header + final_list)

# ==========================================
# 5. 主程序入口
# ==========================================
if __name__ == "__main__":
    print("🌟 啟動【雙引擎量化交易系統】...")
    run_left_side_golden_pit()
    run_right_side_momentum()
    print("\n✅ 所有策略執行完畢！請前往 Google 表格查看。")
