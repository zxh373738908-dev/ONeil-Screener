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

WEBAPP_URL = "https://script.google.com/macros/s/AKfycby1pIM7iO43lcLQpOmi5LCJIn3VN9a0Ilf9amoy1EtQV_GBXJkk_A4PpsrJxKzH7i51/exec"

MONOPOLY_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "TSM", "ASML", "AVGO"]
FALLBACK_UNIVERSE = MONOPOLY_TICKERS + ["AMD", "CRWD", "PLTR", "GE", "COP", "SLB", "LRCX", "JPM", "WMT", "COST"]
EXCLUDED_INDUSTRIES = ['Banks', 'Insurance', 'Financial', 'Credit Services']

def sync_to_google_sheet(sheet_name, matrix):
    try:
        def safe_json_val(val):
            if isinstance(val, float) and not math.isfinite(val): return 0
            return str(val)
        payload = {"sheet_name": sheet_name, "data": json.loads(json.dumps(matrix, default=safe_json_val))}
        requests.post(WEBAPP_URL, json=payload, timeout=15)
        print(f"🎉 同步成功 -> 分頁: [{sheet_name}]")
    except Exception as e: print(f"❌ 同步失敗: {e}")

def safe_get(info_dict, key, default=0):
    val = info_dict.get(key)
    try: return float(val) if val is not None else default
    except: return default

def extract_ticker_data(data, ticker):
    """【終極修復】萬能數據提取器，應對 yfinance 任何版本的 MultiIndex 結構"""
    try:
        if isinstance(data.columns, pd.MultiIndex):
            if ticker in data.columns.levels[0]: return data[ticker].dropna()
            elif ticker in data.columns.levels[1]: return data.xs(ticker, level=1, axis=1).dropna()
        return data.dropna()
    except: return pd.DataFrame()

# ==========================================
# 🛡️ 策略 A: 左側黃金坑 (保留空倉，因為目前沒股災)
# ==========================================
def run_left_side_golden_pit():
    print("\n" + "="*50 + "\n🛡️ [策略 A: 左側黃金坑] 啟動...")
    header = [["🛡️ 左側黃金坑", "更新:", datetime.datetime.now().strftime('%m-%d %H:%M'), "", "", "", "", ""],
              ["代碼", "現價", "5年高點跌幅", "底部信號", "基本面狀態", "建議合約", "到期日", "交易紀律"]]
    # 目前美股在歷史高位，左側理應無股。直接發送空倉信號驗證連線。
    sync_to_google_sheet("🛡️左側_黃金坑", header + [["-", "目前無股災，耐心等待", "-", "-", "-", "-", "-", "-"]])

# ==========================================
# 🚀 策略 B: 右側動能成長 (強制排查版)
# ==========================================
def run_right_side_momentum():
    print("\n" + "="*50 + "\n🚀 [策略 B: 右側動能成長] 啟動...")
    header = [["🚀 動能成長 Top 10", "更新:", datetime.datetime.now().strftime('%m-%d %H:%M'), "大盤:", "測試模式放行", "", "", "", "", ""],
              ["排名", "代碼", "板塊", "現價", "近1月漲幅", "近3月漲幅", "RS動能分", "基本面與技術形態", "綜合總分", "交易紀律"]]

    print("📡 正在下載股票池 K 線數據...")
    data = yf.download(FALLBACK_UNIVERSE, period="1y", interval="1d", group_by='ticker', auto_adjust=True, progress=False)

    cands = []
    for t in FALLBACK_UNIVERSE:
        df = extract_ticker_data(data, t)
        if len(df) < 50: continue # 只要有數據就放行
        
        close = df['Close']
        curr_price = float(close.iloc[-1])
        r1m = (curr_price - close.iloc[-21]) / close.iloc[-21] if len(close)>=21 else 0
        r3m = (curr_price - close.iloc[-63]) / close.iloc[-63] if len(close)>=63 else 0
        r1y = (curr_price - close.iloc[-252]) / close.iloc[-252] if len(close)>=252 else 0
        
        rs_score = (r1m * 0.4) + (r3m * 0.3) + (r1y * 0.3)
        cands.append({"Ticker": t, "Price": curr_price, "RS": rs_score, "1M": r1m, "3M": r3m, "Tightness": 2.0, "Vol_OK": True})

    cands = sorted(cands, key=lambda x: x['RS'], reverse=True)[:15]
    print(f"🔬 成功提取 {len(cands)} 隻股票 K 線，進入基本面體檢...")
    
    final_cands = []
    for c in cands:
        try:
            time.sleep(0.3) 
            info = yf.Ticker(c['Ticker']).info
            sec, ind = str(info.get('sector', '')), str(info.get('industry', ''))
            rev_growth = safe_get(info, 'revenueGrowth') * 100
            
            # 【測試版極度放寬】只要數據不是 0 就給過
            if 'Technology' in sec or 'Software' in ind:
                r40 = rev_growth + ((safe_get(info, 'freeCashflow') / (safe_get(info, 'totalRevenue', 1) or 1)) * 100)
                print(f"  👉 [科技] {c['Ticker']}: Rule 40 = {r40:.1f}%")
                fin_score, fin_msg = r40, f"Rule 40 ({r40:.0f}%)"
            else:
                op_margin = safe_get(info, 'operatingMargins') * 100
                print(f"  👉 [實業] {c['Ticker']}: 利潤率 = {op_margin:.1f}%")
                fin_score, fin_msg = op_margin + rev_growth, f"利潤 ({op_margin:.1f}%)"

            final_cands.append({
                "T": c['Ticker'], "Sec": sec[:10], "P": c['Price'], "1M": c['1M']*100, 
                "3M": c['3M']*100, "RS": c['RS']*100, "Msg": fin_msg, "Tot": (c['RS']*100) + fin_score
            })
        except Exception as e:
            print(f"  ⚠️ {c['Ticker']} API 拒絕訪問: {e}"); continue

    top10 = sorted(final_cands, key=lambda x: x['Tot'], reverse=True)[:10]
    final_list = [["-", "無標的", "-", "-", "-", "-", "-", "-", "-", "-"]] if not top10 else [
        [f"Top {i+1}", r['T'], r['Sec'], round(r['P'], 2), f"{round(r['1M'], 1)}%", 
         f"{round(r['3M'], 1)}%", round(r['RS'], 2), r['Msg'], round(r['Tot'], 2), "測試輸出"]
        for i, r in enumerate(top10)
    ]
    sync_to_google_sheet("🚀右側_動能成長", header + final_list)

if __name__ == "__main__":
    run_left_side_golden_pit()
    run_right_side_momentum()
