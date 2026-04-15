import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import json
import warnings
import math
import random

warnings.filterwarnings('ignore')

# ==========================================
# 1. 系統配置中心
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycby1pIM7iO43lcLQpOmi5LCJIn3VN9a0Ilf9amoy1EtQV_GBXJkk_A4PpsrJxKzH7i51/exec"

MONOPOLY_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "TSM", "ASML", "AVGO",
    "V", "MA", "BRK-B", "SPGI", "MCO", "LLY", "NVO", "UNH", "JNJ", "ISRG",       
    "WMT", "COST", "PG", "KO", "PEP", "LIN", "SHW", "CAT", "DE", "LMT",         
    "UNP", "WM", "RSG", "NOW", "SNPS", "CDNS"
]

FALLBACK_UNIVERSE = MONOPOLY_TICKERS + [
    "AMD", "CRWD", "PLTR", "PANW", "SNOW", "DDOG", "NET", "MDB", "TEAM", "WDAY",
    "ADBE", "CRM", "INTU", "QCOM", "TXN", "AMAT", "LRCX", "KLAC", "MU", "ARM",
    "UBER", "ABNB", "BKNG", "NFLX", "DIS", "CMCSA", "TMUS", "VZ", "T",
    "XOM", "CVX", "COP", "EOG", "SLB", "GE", "RTX", "BA", "HON", "UPS"
]

# 排除容易讓財報數據失真的板塊
EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'Credit Services']

# ==========================================
# 2. 核心防禦工具 (API 裝甲)
# ==========================================
def sync_to_google_sheet(sheet_name, matrix):
    try:
        def safe_json_val(val):
            if isinstance(val, float) and not math.isfinite(val): return 0
            return str(val)
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=safe_json_val))}
        requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 同步成功 -> 分頁: [{sheet_name}]")
    except Exception as e: print(f"❌ 同步失敗 [{sheet_name}]: {e}")

def safe_get(info_dict, key, default=0):
    val = info_dict.get(key)
    try: return float(val) if val is not None else default
    except: return default

def extract_ticker_data(data, ticker):
    """萬能數據提取器，應對 yfinance 各種結構變化"""
    try:
        if isinstance(data.columns, pd.MultiIndex):
            if ticker in data.columns.levels[0]: return data[ticker].dropna()
            elif ticker in data.columns.levels[1]: return data.xs(ticker, level=1, axis=1).dropna()
        return data.dropna()
    except: return pd.DataFrame()

# ==========================================
# 3. 🛡️ 策略 A: 左側黃金坑 (實戰版)
# ==========================================
def run_left_side_golden_pit():
    print("\n" + "="*50 + "\n🛡️ [策略 A: 左側黃金坑] 啟動掃描...")
    
    try: data = yf.download(MONOPOLY_TICKERS, period="5y", interval="1d", group_by='ticker', auto_adjust=True, progress=False)
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
            drawdown = (curr_price - float(df_w['High'].max())) / float(df_w['High'].max())
            
            if drawdown > -0.30: continue # 跌幅不夠深
            if curr_price < ma10: continue # 還沒止跌企穩
            
            # 放寬巨量標準至 1.5倍 均量
            if vol.tail(20).max() < (vol.tail(60).mean() * 1.5): continue 
            
            time.sleep(0.2)
            info = yf.Ticker(t).info
            if safe_get(info, 'freeCashflow') > 0 and safe_get(info, 'revenueGrowth') > -0.10:
                candidates.append({
                    "Ticker": t, "Price": curr_price, "Drawdown": drawdown*100,
                    "Status": "FCF健康 | 10MA企穩 | 📈放量見底", "Strike": curr_price * 0.80
                })
        except: continue

    header = [
        ["🛡️ 左側黃金坑 (價值回歸)", "更新:", datetime.datetime.now().strftime('%m-%d %H:%M'), "策略:", "極端錯殺", "", "", ""],
        ["代碼", "現價", "5年高點跌幅", "底部信號", "基本面狀態", "建議合約", "到期日", "交易紀律"]
    ]
    final_list = [["-", "市場處於高位，無符合標的", "-", "-", "-", "-", "-", "-"]] if not candidates else [
        [r['Ticker'], round(r['Price'], 2), f"{round(r['Drawdown'], 2)}%", "爆量+企穩", r['Status'], 
         f"Deep ITM Call @ ${round(r['Strike'], 2)}", "> 360 Days", "翻倍平半 / 剩120天平倉"]
        for r in sorted(candidates, key=lambda x: x['Drawdown'])
    ]
    sync_to_google_sheet("🛡️左側_黃金坑", header + final_list)

# ==========================================
# 4. 🚀 策略 B: 右側動能成長 (實戰均衡版)
# ==========================================
def run_right_side_momentum():
    print("\n" + "="*50 + "\n🚀 [策略 B: 右側動能成長] 啟動掃描...")
    
    try:
        spy = extract_ticker_data(yf.download("SPY", period="6mo", progress=False), "SPY") if "SPY" in yf.download("SPY", period="6mo", progress=False).columns else yf.download("SPY", period="6mo", progress=False)
        is_bull = float(spy['Close'].iloc[-1]) > float(spy['Close'].tail(50).mean())
    except: is_bull = True

    header = [
        ["🚀 動能成長 Top 10", "更新:", datetime.datetime.now().strftime('%m-%d %H:%M'), "大盤:", "🟢 多頭允許交易" if is_bull else "🔴 轉弱暫停交易", "", "", "", "", ""],
        ["排名", "代碼", "板塊", "現價", "近1月漲幅", "近3月漲幅", "RS動能分", "基本面與技術形態", "綜合總分", "交易紀律"]
    ]

    if not is_bull:
        sync_to_google_sheet("🚀右側_動能成長", header + [["-", "大盤轉弱", "-", "-", "-", "-", "-", "保留現金", "-", "觀望"]])
        return

    try: tickers = [t.replace('.', '-') for t in pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]['Symbol'].tolist()]
    except: tickers = FALLBACK_UNIVERSE

    print("📡 正在下載股票池 K 線數據...")
    try: data = yf.download(tickers, period="1y", interval="1d", group_by='ticker', auto_adjust=True, threads=True, progress=False)
    except: return

    cands = []
    for t in tickers:
        try:
            df = extract_ticker_data(data, t)
            if len(df) < 100: continue
            
            close, vol = df['Close'], df['Volume']
            curr_price, ma50 = float(close.iloc[-1]), close.tail(50).mean()
            
            # 【實戰條件】只要站上 50MA 且沒有乖離大於 35% 就視為多頭
            if curr_price < ma50 or ((curr_price - ma50) / ma50) > 0.35: continue
            
            r1m = (curr_price - close.iloc[-21]) / close.iloc[-21] if len(close)>=21 else 0
            r3m = (curr_price - close.iloc[-63]) / close.iloc[-63] if len(close)>=63 else 0
            r1y = (curr_price - close.iloc[-252]) / close.iloc[-252] if len(close)>=252 else 0
            
            rs_score = (r1m * 0.4) + (r3m * 0.3) + (r1y * 0.3)
            if rs_score > 0:
                cands.append({"Ticker": t, "Price": curr_price, "RS": rs_score, "1M": r1m, "3M": r3m, 
                              "Tightness": (close.tail(15).std() / close.tail(15).mean()) * 100, 
                              "Vol_OK": vol.iloc[-1] > vol.tail(10).mean()})
        except: continue

    cands = sorted(cands, key=lambda x: x['RS'], reverse=True)[:30] # 減少請求量，提高穩定性
    print(f"🔬 進入基本面體檢 (共 {len(cands)} 隻)，啟動反封鎖延遲...")
    
    final_cands = []
    for c in cands:
        try:
            time.sleep(random.uniform(0.2, 0.4)) 
            info = yf.Ticker(c['Ticker']).info
            
            sec, ind = str(info.get('sector', '')), str(info.get('industry', ''))
            # 【關鍵】重新啟動金融股剔除過濾器
            if any(ex.lower() in sec.lower() or ex.lower() in ind.lower() for ex in EXCLUDED_INDUSTRIES): continue
            
            fin_score, fin_msg = 0, ""
            rev_growth = safe_get(info, 'revenueGrowth') * 100
            
            if 'Technology' in sec or 'Software' in ind:
                r40 = rev_growth + ((safe_get(info, 'freeCashflow') / (safe_get(info, 'totalRevenue', 1) or 1)) * 100)
                if r40 >= 30.0:  # 【實戰】放寬至 30%
                    fin_score, fin_msg = r40, f"Rule 40 ({r40:.0f}%)"
            else:
                op_margin = safe_get(info, 'operatingMargins') * 100
                if op_margin >= 10.0 and rev_growth > -5.0: # 【實戰】利潤率大於 10%
                    fin_score, fin_msg = op_margin + rev_growth, f"利潤 ({op_margin:.1f}%)"

            if fin_score > 0:
                tight_msg = f" | 收斂:{c['Tightness']:.1f}%" if c['Tightness'] < 4.0 else ""
                vol_msg = " 📈放量" if c['Vol_OK'] else ""
                final_cands.append({
                    "T": c['Ticker'], "Sec": sec[:10], "P": c['Price'], "1M": c['1M']*100, 
                    "3M": c['3M']*100, "RS": c['RS']*100, "Msg": fin_msg + tight_msg + vol_msg,
                    "Tot": (c['RS']*100) + fin_score - (c['Tightness'] * 1.5) 
                })
        except Exception as e: continue

    top10 = sorted(final_cands, key=lambda x: x['Tot'], reverse=True)[:10]
    final_list = [["-", "無符合標的", "-", "-", "-", "-", "-", "-", "-", "-"]] if not top10 else [
        [f"Top {i+1}", r['T'], r['Sec'], round(r['P'], 2), f"{round(r['1M'], 1)}%", 
         f"{round(r['3M'], 1)}%", round(r['RS'], 2), r['Msg'], round(r['Tot'], 2), "回踩 20EMA 買入 / 破 50MA 止損"]
        for i, r in enumerate(top10)
    ]
    sync_to_google_sheet("🚀右側_動能成長", header + final_list)

if __name__ == "__main__":
    print("🌟 啟動【雙引擎量化交易系統 V5 - 最終實戰版】...")
    run_left_side_golden_pit()
    run_right_side_momentum()
    print("\n✅ 所有策略執行完畢！")
