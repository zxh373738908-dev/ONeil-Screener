import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import yfinance as yf
import requests
import re
import time
from gspread_formatting import *

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (请务必核对您的 GID)
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_GID = 665566258  
CREDS_FILE = "credentials.json"
ACCOUNT_SIZE = 1000000 
MAX_RISK_PER_TRADE = 0.008 
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_commander_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    for ws in doc.worksheets():
        if str(ws.id) == str(TARGET_GID): return ws
    return doc.get_worksheet(0)

# ==========================================
# 🧠 2. 量子统帅核心引擎
# ==========================================
def calculate_quantum_commander(df, hsi_series, hstech_ok):
    try:
        df = df.dropna(subset=['Close'])
        if len(df) < 150: return None
        
        close = df['Close'].values.astype(float)
        high = df['High'].values.astype(float)
        low = df['Low'].values.astype(float)
        vol = df['Volume'].values.astype(float)
        turnover = close * vol
        cp = close[-1]
        
        # 1. 流动性过滤 (如果没数据，可能是这里门槛太高)
        avg_turnover_20d = np.mean(turnover[-20:])
        if avg_turnover_20d < 80000000: return "LOW_LIQUID" # 降至8000万便于调试

        ma50 = np.mean(close[-50:])
        ma200 = np.mean(close[-200:])
        is_stage_2 = (cp > ma50) # 宽松Stage 2判定便于观察
        
        # 2. RS 矩阵
        # 确保日期对齐
        common_idx = hsi_series.index.intersection(df.index)
        if len(common_idx) < 10: return None
        
        rs_line = close[df.index.get_indexer(common_idx)] / hsi_series.loc[common_idx].values
        rs_turbo = (rs_line[-1] / rs_line[-6] - 1) * 100 if len(rs_line) > 6 else 0
        rs_nh = rs_line[-1] >= np.max(rs_line[-100:])

        # 3. 筹码与紧致度
        price_bins = np.linspace(np.min(low[-100:]), np.max(high[-100:]), 40)
        hist, _ = np.histogram(close[-100:], bins=price_bins, weights=vol[-100:])
        poc_price = price_bins[np.argmax(hist)]
        
        tightness = (np.std(close[-10:]) / np.mean(close[-10:])) * 100
        
        # 4. 口袋枢轴
        neg_vol = vol[-11:-1][close[-11:-1] < close[-12:-2]]
        max_neg_vol = np.max(neg_vol) if len(neg_vol) > 0 else 9e12
        is_pocket = (close[-1] > close[-2]) and (vol[-1] > max_neg_vol)

        # 5. 战法决策
        dist_ma50 = (cp / ma50 - 1) * 100
        action, score = "观察", 60
        
        if dist_ma50 > 15: action, score = "⚠️ 乖離過大", 40
        elif is_pocket and cp > poc_price and tightness < 2.0: action, score = "🎯 領袖口袋(Pocket)", 95
        elif rs_nh and cp >= np.max(close[-20:]): action, score = "🚀 巔峰突破(Breakout)", 92
        elif tightness < 1.2 and vol[-1] < np.mean(vol[-20:])*0.6: action, score = "💎 极致收缩(VCP)", 88

        if not hstech_ok: score -= 15
        
        # 6. 风险头寸
        adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
        stop_price = max(ma50 * 0.985, cp * (1 - adr_20 * 0.01 * 1.6))
        shares = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // (cp - stop_price) if cp > stop_price else 0

        return {
            "Action": action, "Score": score, "Price": cp, "Dist_50": round(dist_ma50, 1),
            "Shares": int(shares), "Stop": round(stop_price, 2), "Tight": round(tightness, 2),
            "RS_Turbo": round(rs_turbo, 2), "Turnover_M": round(avg_turnover_20d/1e6, 1),
            "Above_POC": "✅" if cp > poc_price else "-", "rs_raw": cp/close[-120] if len(close)>120 else 1,
            "is_stage_2": is_stage_2
        }
    except: return None

# ==========================================
# 🚀 3. 主程序
# ==========================================
def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ 量子统帅系统启动...")
    
    try:
        sh = init_commander_sheet()
    except Exception as e:
        print(f"❌ 无法连接 Google 表格: {e}"); return

    # 1. 环境审计
    mkt_data = yf.download(["^HSI", "3088.HK"], period="60d", progress=False)['Close']
    hsi_series = mkt_data["^HSI"].dropna()
    hstech = mkt_data["3088.HK"].dropna()
    hstech_ok = hstech.iloc[-1] > hstech.rolling(20).mean().iloc[-1] if not hstech.empty else False
    mkt_weather = "☀️ 激进" if hstech_ok else "☁️ 谨慎"

    # 更新心跳，证明程序在跑
    sh.update(range_name="A1", values=[[f"🏯 量子统帅运行中", f"天气: {mkt_weather}", f"最后刷新: {now_str}", "状态: 正在审计数据..."]])

    # 2. 获取票池
    headers = {"User-Agent": "Mozilla/5.0"}
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "description", "close", "market_cap_basic", "sector"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1e10}],
               "range": [0, 300], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "其他"} for d in resp])
        tickers = [c.zfill(4)+".HK" for c in df_pool['code']]
    except Exception as e:
        print(f"❌ 无法获取初筛列表: {e}"); return

    # 3. 批量下载
    print(f"🔎 正在审计 {len(tickers)} 只标的...")
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    final_list = []
    low_liquid_count = 0
    
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            res = calculate_quantum_commander(data[t], hsi_series, hstech_ok)
            
            if res == "LOW_LIQUID": 
                low_liquid_count += 1; continue
            
            if res and res['Action'] != "观察" and res['is_stage_2']:
                code_clean = t.split('.')[0].lstrip('0')
                # 修复 Sector 匹配逻辑
                sector_search = df_pool[df_pool['code'] == code_clean]
                sector = sector_search['sector'].iloc[0] if not sector_search.empty else "领袖"
                res.update({"Ticker": t.split('.')[0], "Sector": sector})
                final_list.append(res)
        except: continue

    print(f"📊 审计结束：{low_liquid_count} 只因成交额不足被剔除，{len(final_list)} 只符合战法。")

    # 4. 最终写入
    sh.clear()
    sh.update(range_name="A1", values=[[f"🏯 量子统帅旗舰版", f"环境: {mkt_weather}", f"刷新: {now_str}", f"有效信号: {len(final_list)}"]])
    
    if final_list:
        res_df = pd.DataFrame(final_list)
        res_df['RS_Rank'] = res_df['rs_raw'].rank(pct=True).apply(lambda x: int(x*99))
        res_df = res_df.sort_values(by="Score", ascending=False).groupby("Sector").head(3)
        
        cols = ["Ticker", "Action", "RS_Rank", "Score", "Price", "Shares", "Stop", "Tight", "Dist_50", "RS_Turbo", "Sector"]
        sh.update(range_name="A3", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
        
        # 格式化
        set_frozen(sh, rows=3)
        format_cell_range(sh, 'A3:K3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0,0,0)))
        print(f"✅ Google Sheet 已同步。")
    else:
        sh.update_acell("A4", "📭 今日暂无符合顶级形态标的 (1.已满足Stage2趋势 2.已满足8000万成交额 3.暂无VCP或口袋枢轴)")
        print(f"📭 无数据写入。")

if __name__ == "__main__":
    main()
