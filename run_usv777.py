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
WEBAPP_URL = "https://script.google.com/macros/s/AKfycby1pIM7iO43lcLQpOmi5LCJIn3VN9a0Ilf9amoy1EtQV_GBXJkk_A4PpsrJxKzH7i51/exec"

# 左側：嚴格的壟斷巨頭名單
MONOPOLY_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "TSM", "ASML", "AVGO",
    "V", "MA", "JPM", "BRK-B", "SPGI", "MCO", "LLY", "NVO", "UNH", "JNJ", "ISRG",       
    "WMT", "COST", "PG", "KO", "PEP", "LIN", "SHW", "CAT", "DE", "LMT",         
    "UNP", "WM", "RSG", "NOW", "SNPS", "CDNS"
]

# 右側：超強備用池 (納斯達克核心+標普代表)
FALLBACK_UNIVERSE = MONOPOLY_TICKERS + [
    "AMD", "CRWD", "PLTR", "PANW", "SNOW", "DDOG", "NET", "MDB", "TEAM", "WDAY",
    "ADBE", "CRM", "INTU", "QCOM", "TXN", "AMAT", "LRCX", "KLAC", "MU", "ARM",
    "UBER", "ABNB", "BKNG", "NFLX", "DIS", "CMCSA", "TMUS", "VZ", "T",
    "JPM", "BAC", "WFC", "GS", "MS", "AXP", "BLK", "C", "PGR", "CB",
    "XOM", "CVX", "COP", "EOG", "SLB", "GE", "RTX", "BA", "HON", "UPS"
]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'Credit Services']

# ==========================================
# 2. 通用工具與 API 裝甲
# ==========================================
def sync_to_google_sheet(sheet_name, matrix):
    try:
        def safe_json_val(val):
            if isinstance(val, float) and not math.isfinite(val): return 0
            return str(val)
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=safe_json_val))}
        requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 同步成功 -> 分頁: [{sheet_name}]")
    except Exception as e:
        print(f"❌ 同步失敗 [{sheet_name}]: {e}")

def safe_get(info_dict, key, default=0):
    val = info_dict.get(key)
    try: return float(val) if val is not None else default
    except: return default

def extract_ticker_data(data, ticker):
    """【改進3】裝甲級數據提取，無視 MultiIndex 報錯"""
    try:
        if isinstance(data.columns, pd.MultiIndex):
            return data.xs(ticker, level=1, axis=1).dropna()
        else:
            return data.dropna() if len(data.columns) <= 6 else pd.DataFrame()
    except:
        return pd.DataFrame()

def calculate_weekly_rsi(prices, period=14):
    if len(prices) < period + 1: return 50.0
    delta = prices.diff()
    up = delta.clip(lower=0).ewm(com=period-1, adjust=False).mean()
    down = (-1 * delta.clip(upper=0)).ewm(com=period-1, adjust=False).mean()
    rs = up / down
    return float((100 - (100 / (1 + rs))).iloc[-1])

# ==========================================
# 3. 🛡️ 策略 A: 左側黃金坑 (巨量見底版)
# ==========================================
def run_left_side_golden_pit():
    print("\n" + "="*50)
    print("🛡️ [策略 A: 左側黃金坑] 啟動掃描...")
    
    try:
        data = yf.download(MONOPOLY_TICKERS, period="max", interval="1d", group_by='ticker', auto_adjust=True, progress=False)
    except: return

    candidates = []
    for t in MONOPOLY_TICKERS:
        try:
            df = extract_ticker_data(data, t)
            if len(df) < 250: continue
            
            close, vol = df['Close'], df['Volume']
            curr_price = float(close.iloc[-1])
            ma10 = close.tail(10).mean()
            
            df_w = df.resample('W-FRI').agg({'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()
            ath = float(df_w['High'].max())
            drawdown = (curr_price - ath) / ath
            
            if drawdown > -0.30: continue
            if calculate_weekly_rsi(df_w['Close'], 14) >= 30: continue
            if curr_price < ma10: continue 
            
            # 【改進1】恐慌衰竭量檢測：過去 20 天內，是否出現過成交量大於 60天均量 2 倍的日子
            vol_60_ma = vol.tail(60).mean()
            recent_max_vol = vol.tail(20).max()
            if recent_max_vol < (vol_60_ma * 2.0): continue # 沒有巨量承接，不抄底
            
            info = yf.Ticker(t).info
            fcf = safe_get(info, 'freeCashflow')
            roe = safe_get(info, 'returnOnEquity')
            rev_growth = safe_get(info, 'revenueGrowth')
            
            if fcf > 0 and roe > 0 and rev_growth > -0.10:
                candidates.append({
                    "Ticker": t, "Price": curr_price, "Drawdown": drawdown*100,
                    "Status": f"FCF健康 | 10MA企穩 | 📈巨量見底", 
                    "Strike": curr_price * 0.80
                })
        except: continue

    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["🛡️ 左側黃金坑 (價值回歸)", "更新:", bj_now, "策略:", "極端錯殺/Deep ITM期權", "", "", ""],
        ["代碼", "現價", "距高點跌幅", "底部信號", "基本面狀態", "建議買入合約", "建議到期日", "交易紀律"]
    ]
    
    final_list = []
    if not candidates:
        final_list.append(["-", "等待股災與巨量承接信號", "-", "-", "-", "-", "-", "-"])
    else:
        for r in sorted(candidates, key=lambda x: x['Drawdown']):
            final_list.append([
                r['Ticker'], round(r['Price'], 2), f"{round(r['Drawdown'], 2)}%",
                "爆量 + 站上10MA", r['Status'], f"Deep ITM Call @ ${round(r['Strike'], 2)}",
                "> 360 Days", "翻倍平半 / 剩120天平倉 / 零止損"
            ])
    sync_to_google_sheet("🛡️左側_黃金坑", header + final_list)

# ==========================================
# 4. 🚀 策略 B: 右側動能成長 (硬核利潤版)
# ==========================================
def run_right_side_momentum():
    print("\n" + "="*50)
    print("🚀 [策略 B: 右側動能成長] 啟動掃描...")
    
    try:
        spy = yf.download("SPY", period="6mo", progress=False)['Close']
        if isinstance(spy, pd.DataFrame): spy = spy.iloc[:, 0]
        is_bull = float(spy.iloc[-1]) > float(spy.tail(50).mean())
    except: is_bull = True

    bj_now = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%H:%M:%S')
    header = [
        ["🚀 動能成長 Top 10", "更新:", bj_now, "大盤狀態:", "🟢 多頭允許交易" if is_bull else "🔴 轉弱暫停交易", "", "", "", "", ""],
        ["排名", "代碼", "板塊", "現價", "近1月漲幅", "近3月漲幅", "RS動能分", "硬核基本面與形態", "綜合總分", "交易紀律"]
    ]

    if not is_bull:
        sync_to_google_sheet("🚀右側_動能成長", header + [["-", "大盤轉弱", "-", "-", "-", "-", "-", "保留現金", "-", "觀望 / 嚴禁追高"]])
        return

    try:
        tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        tickers = [t.replace('.', '-') for t in tables[0]['Symbol'].tolist()]
    except: tickers = FALLBACK_UNIVERSE

    try:
        data = yf.download(tickers, period="1y", interval="1d", group_by='ticker', auto_adjust=True, threads=True, progress=False)
    except: return

    cands = []
    for t in tickers:
        try:
            df = extract_ticker_data(data, t)
            if len(df) < 200: continue
            
            close, vol = df['Close'], df['Volume']
            curr_price = float(close.iloc[-1])
            ma50, ma200 = close.tail(50).mean(), close.tail(200).mean()
            
            # 過濾乖離率妖股
            if curr_price < ma50 or ma50 < ma200 or ((curr_price - ma50) / ma50) > 0.35: continue
            
            tightness = (close.tail(15).std() / close.tail(15).mean()) * 100
            vol_recent, vol_ma10 = vol.iloc[-1], vol.tail(10).mean()
            if vol_ma10 < 500_000: continue
            
            r1m = (curr_price - close.iloc[-21]) / close.iloc[-21] if len(close)>=21 else 0
            r3m = (curr_price - close.iloc[-63]) / close.iloc[-63] if len(close)>=63 else 0
            r1y = (curr_price - close.iloc[-252]) / close.iloc[-252] if len(close)>=252 else 0
            
            rs_score = (r1m * 0.4) + (r3m * 0.3) + (r1y * 0.3)
            if rs_score < 0: continue
            
            cands.append({
                "Ticker": t, "Price": curr_price, "RS": rs_score, "1M": r1m, "3M": r3m, 
                "Vol_OK": vol_recent > vol_ma10, "Tightness": tightness
            })
        except: continue

    cands = sorted(cands, key=lambda x: x['RS'], reverse=True)[:40]
    final_cands = []
    
    for c in cands:
        try:
            info = yf.Ticker(c['Ticker']).info
            sec, ind = info.get('sector', ''), info.get('industry', '')
            if any(ex.lower() in sec.lower() or ex.lower() in ind.lower() for ex in EXCLUDED_INDUSTRIES): continue
            
            fin_score, fin_msg = 0, ""
            
            # 【改進2】科技股用 Rule 40，傳統股用「營業利潤率+正增長」
            if 'Technology' in sec or 'Software' in ind:
                r40 = (safe_get(info, 'revenueGrowth') + (safe_get(info, 'freeCashflow')/(safe_get(info, 'totalRevenue', 1) or 1))) * 100
                if r40 >= 40: fin_score, fin_msg = r40, f"Rule 40 ({r40:.0f}%)"
            else:
                op_margin = safe_get(info, 'operatingMargins') * 100
                rev_growth = safe_get(info, 'revenueGrowth') * 100
                if op_margin > 10.0 and rev_growth > 0:
                    fin_score, fin_msg = op_margin + rev_growth, f"高利潤 ({op_margin:.1f}%)"

            if fin_score > 0:
                tight_msg = f" | 收斂:{c['Tightness']:.1f}%"
                vol_msg = " 📈放量" if c['Vol_OK'] else ""
                final_cands.append({
                    "T": c['Ticker'], "Sec": sec, "P": c['Price'], "1M": c['1M']*100, 
                    "3M": c['3M']*100, "RS": c['RS']*100, "Msg": fin_msg + tight_msg + vol_msg,
                    "Tot": (c['RS']*100) + fin_score - (c['Tightness'] * 2) 
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
                "突破收斂區買入 / 破 50MA 止損"
            ])
            
    sync_to_google_sheet("🚀右側_動能成長", header + final_list)

if __name__ == "__main__":
    print("🌟 啟動【雙引擎量化交易系統 V3 - 機構實戰版】...")
    run_left_side_golden_pit()
    run_right_side_momentum()
    print("\n✅ 所有策略執行完畢！請前往 Google 表格查看。")
