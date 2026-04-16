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
    "UBER", "ABNB", "BKNG", "NFLX", "DIS", "CMCSA", "TMUS", "T",
    "XOM", "CVX", "COP", "EOG", "SLB", "GE", "RTX", "BA", "HON", "UPS"
]

EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'Credit Services']
MAX_PER_SECTOR = 3  

# ==========================================
# 2. 核心防禦工具 & API 裝甲
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
    try:
        if isinstance(data.columns, pd.MultiIndex):
            if ticker in data.columns.levels[0]: return data[ticker].dropna()
            elif ticker in data.columns.levels[1]: return data.xs(ticker, level=1, axis=1).dropna()
        return data.dropna()
    except: return pd.DataFrame()

# 獲取大盤與恐慌指數
def get_market_regime():
    try:
        spy_df = extract_ticker_data(yf.download("SPY", period="6mo", progress=False), "SPY") if "SPY" in yf.download("SPY", period="6mo", progress=False).columns else yf.download("SPY", period="6mo", progress=False)
        vix_df = extract_ticker_data(yf.download("^VIX", period="1mo", progress=False), "^VIX") if "^VIX" in yf.download("^VIX", period="1mo", progress=False).columns else yf.download("^VIX", period="1mo", progress=False)
        
        curr_spy, ma50_spy = float(spy_df['Close'].iloc[-1]), float(spy_df['Close'].tail(50).mean())
        curr_vix = float(vix_df['Close'].iloc[-1])
        
        # 【優化1】VIX 必須小於 22 且 SPY 站上 50MA 才是真正的安全牛市
        is_bull = (curr_spy > ma50_spy) and (curr_vix < 22)
        return is_bull, curr_vix
    except: return True, 15.0

# ==========================================
# 3. 🛡️ 策略 A: 左側黃金坑 (智能 IV 防護版)
# ==========================================
def run_left_side_golden_pit(curr_vix):
    print("\n" + "="*50 + "\n🛡️ [策略 A: 左側黃金坑] 啟動掃描...")
    
    try: data = yf.download(MONOPOLY_TICKERS, period="5y", interval="1d", group_by='ticker', auto_adjust=True, progress=False)
    except: return

    candidates = []
    for t in MONOPOLY_TICKERS:
        try:
            df = extract_ticker_data(data, t)
            if len(df) < 250: continue
            
            close, vol = df['Close'], df['Volume']
            curr_price, ma20 = float(close.iloc[-1]), close.tail(20).mean()
            
            df_w = df.resample('W-FRI').agg({'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()
            drawdown = (curr_price - float(df_w['High'].max())) / float(df_w['High'].max())
            
            if drawdown > -0.30 or curr_price < ma20: continue 
            if vol.tail(20).max() < (vol.tail(60).mean() * 1.5): continue 
            
            time.sleep(0.2)
            info = yf.Ticker(t).info
            if safe_get(info, 'freeCashflow') > 0 and safe_get(info, 'revenueGrowth') > -0.10:
                # 【優化3】高 VIX 環境下提示改用 Spread 策略
                discipline = "翻倍平半 / 零止損" if curr_vix < 25 else "⚠️IV極高！改用 Bull Call Spread"
                
                candidates.append({
                    "Ticker": t, "Price": curr_price, "Drawdown": drawdown*100,
                    "Status": "FCF健康 | 20MA企穩 | 📈放量", "Strike": curr_price * 0.80, "Discipline": discipline
                })
        except: continue

    header = [
        ["🛡️ 左側黃金坑", "更新:", datetime.datetime.now().strftime('%m-%d %H:%M'), "VIX:", f"{curr_vix:.1f}", "", "", ""],
        ["代碼", "現價", "5年跌幅", "底部信號", "基本面狀態", "建議合約", "到期日", "交易紀律"]
    ]
    final_list = [["-", "市場處於高位，耐心等待", "-", "-", "-", "-", "-", "-"]] if not candidates else [
        [r['Ticker'], round(r['Price'], 2), f"{round(r['Drawdown'], 2)}%", "爆量+20MA企穩", r['Status'], 
         f"Deep ITM Call @ ${round(r['Strike'], 2)}", "> 360 Days", r['Discipline']]
        for r in sorted(candidates, key=lambda x: x['Drawdown'])
    ]
    sync_to_google_sheet("🛡️左側_黃金坑", header + final_list)

# ==========================================
# 4. 🚀 策略 B: 右側動能成長 (智能狙擊區版)
# ==========================================
def run_right_side_momentum(is_bull, curr_vix):
    print("\n" + "="*50 + "\n🚀 [策略 B: 右側動能成長] 啟動掃描...")
    
    market_status = f"🟢 允許交易 (VIX:{curr_vix:.1f})" if is_bull else f"🔴 轉弱或恐慌 (VIX:{curr_vix:.1f})，暫停"
    header = [
        ["🚀 動能成長 Top 10", "更新:", datetime.datetime.now().strftime('%m-%d %H:%M'), "大盤:", market_status, "", "", "", "", ""],
        ["排名", "代碼", "板塊", "現價", "近1月漲幅", "近3月漲幅", "RS動能分", "基本面與技術形態", "綜合總分", "交易紀律"]
    ]

    if not is_bull:
        sync_to_google_sheet("🚀右側_動能成長", header + [["-", "環境惡劣", "-", "-", "-", "-", "-", "保留現金", "-", "嚴禁追高"]])
        return

    try: tickers = [t.replace('.', '-') for t in pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]['Symbol'].tolist()]
    except: tickers = FALLBACK_UNIVERSE

    print("📡 下載股票池 K 線...")
    try: data = yf.download(tickers, period="1y", interval="1d", group_by='ticker', auto_adjust=True, threads=True, progress=False)
    except: return

    cands = []
    for t in tickers:
        try:
            df = extract_ticker_data(data, t)
            if len(df) < 100: continue
            
            close, vol = df['Close'], df['Volume']
            curr_price, ma50 = float(close.iloc[-1]), close.tail(50).mean()
            
            if curr_price < ma50 or ((curr_price - ma50) / ma50) > 0.35: continue
            
            # 【優化2】計算真正的 20EMA 並判斷是否進入「狙擊區」 (±2.5% 範圍)
            ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
            dist_to_ema20 = abs(curr_price - ema20) / ema20
            in_sniper_zone = dist_to_ema20 <= 0.025 
            
            r1m = (curr_price - close.iloc[-21]) / close.iloc[-21] if len(close)>=21 else 0
            r3m = (curr_price - close.iloc[-63]) / close.iloc[-63] if len(close)>=63 else 0
            r1y = (curr_price - close.iloc[-252]) / close.iloc[-252] if len(close)>=252 else 0
            
            rs_score = (r1m * 0.4) + (r3m * 0.3) + (r1y * 0.3)
            if rs_score > 0:
                cands.append({"Ticker": t, "Price": curr_price, "RS": rs_score, "1M": r1m, "3M": r3m, 
                              "Tightness": (close.tail(15).std() / close.tail(15).mean()) * 100, 
                              "Vol_OK": vol.iloc[-1] > vol.tail(10).mean(),
                              "Sniper": in_sniper_zone}) # 記錄狙擊狀態
        except: continue

    cands = sorted(cands, key=lambda x: x['RS'], reverse=True)[:50] 
    print(f"🔬 體檢 (共 {len(cands)} 隻)，執行均衡與狙擊偵測...")
    
    final_cands = []
    for c in cands:
        try:
            time.sleep(random.uniform(0.1, 0.3)) 
            info = yf.Ticker(c['Ticker']).info
            
            sec, ind = str(info.get('sector', '')), str(info.get('industry', ''))
            if any(ex.lower() in sec.lower() or ex.lower() in ind.lower() for ex in EXCLUDED_INDUSTRIES): continue
            
            fin_score, fin_msg = 0, ""
            rev_growth = safe_get(info, 'revenueGrowth') * 100
            
            if 'Technology' in sec or 'Software' in ind:
                r40 = rev_growth + ((safe_get(info, 'freeCashflow') / (safe_get(info, 'totalRevenue', 1) or 1)) * 100)
                if r40 >= 30.0: fin_score, fin_msg = r40, f"Rule 40 ({r40:.0f}%)"
            else:
                op_margin = safe_get(info, 'operatingMargins') * 100
                if op_margin >= 10.0 and rev_growth > -5.0: fin_score, fin_msg = op_margin + rev_growth, f"利潤 ({op_margin:.1f}%)"

            if fin_score > 0:
                tight_msg = f" | 收斂:{c['Tightness']:.1f}%" if c['Tightness'] < 4.0 else ""
                vol_msg = " 📈放量" if c['Vol_OK'] else ""
                final_cands.append({
                    "T": c['Ticker'], "Sec": sec.replace("Consumer Defensive", "Cons Def").replace("Consumer Cyclical", "Cons Cyc")[:12], 
                    "P": c['Price'], "1M": c['1M']*100, "3M": c['3M']*100, "RS": c['RS']*100, 
                    "Msg": fin_msg + tight_msg + vol_msg,
                    "Tot": (c['RS']*100) + fin_score - (c['Tightness'] * 1.5),
                    "Sniper": c['Sniper']
                })
        except: continue

    sorted_all = sorted(final_cands, key=lambda x: x['Tot'], reverse=True)
    top10, sector_counts = [], {}
    
    for r in sorted_all:
        sec = r['Sec']
        if sector_counts.get(sec, 0) < MAX_PER_SECTOR:
            top10.append(r)
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
        if len(top10) >= 10: break

    final_list = [["-", "無符合標的", "-", "-", "-", "-", "-", "-", "-", "-"]] if not top10 else [
        [f"Top {i+1}", r['T'], r['Sec'], round(r['P'], 2), f"{round(r['1M'], 1)}%", 
         f"{round(r['3M'], 1)}%", round(r['RS'], 2), r['Msg'], round(r['Tot'], 2), 
         "🎯 回踩到位！現價買入" if r['Sniper'] else "等待回踩 20EMA"] # 【優化2】智能狙擊信號
        for i, r in enumerate(top10)
    ]
    sync_to_google_sheet("🚀右側_動能成長", header + final_list)

if __name__ == "__main__":
    print("🌟 啟動【雙引擎量化系統 V9.0 - 智能狙擊版】...")
    is_bull, curr_vix = get_market_regime()
    run_left_side_golden_pit(curr_vix)
    run_right_side_momentum(is_bull, curr_vix)
    print("\n✅ 所有策略執行完畢！")
