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

# 基础设置
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心 (请核对您的 SS_KEY 和 GID)
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_GID = 665566258  
CREDS_FILE = "credentials.json"
ACCOUNT_SIZE = 1000000 # 100万港币基准
MAX_RISK_PER_TRADE = 0.008 # 单笔风险 0.8%
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
# 🧠 2. 量子统帅演算引擎 (Flagship Engine)
# ==========================================
def calculate_quantum_commander(df, hsi_series, hstech_ok):
    try:
        df = df.dropna(subset=['Close'])
        if len(df) < 250: return None
        
        close = df['Close'].values.astype(float)
        high = df['High'].values.astype(float)
        low = df['Low'].values.astype(float)
        vol = df['Volume'].values.astype(float)
        turnover = close * vol
        cp = close[-1]
        
        # --- A. 流动性与均线判定 (哨兵防御) ---
        avg_turnover_20d = np.mean(turnover[-20:])
        if avg_turnover_20d < 120000000: return None # 1.2亿硬门槛

        ma50, ma200 = np.mean(close[-50:]), np.mean(close[-200:])
        dist_ma50 = (cp / ma50 - 1) * 100 # 乖离率判定
        
        # --- B. RS 矩阵 (领袖进攻) ---
        # 1. 长线 RS Raw (IBD 风格)
        def get_ret(d): return cp / close[-d] if len(close) >= d else 1
        rs_long = (get_ret(63)*4 + get_ret(126)*3 + get_ret(189)*2 + get_ret(252)*1)
        
        # 2. 短线 RS Turbo (哨兵提速)
        hsi_val = hsi_series.reindex(df.index).ffill().values
        rs_line = close / hsi_val
        rs_turbo = (rs_line[-1] / rs_line[-6] - 1) * 100 # 近5日斜率
        rs_nh = rs_line[-1] >= np.max(rs_line[-252:])

        # --- C. 筹码与量能 (量子核心) ---
        # 1. POC 探测 (100日筹码中心)
        price_bins = np.linspace(np.min(low[-100:]), np.max(high[-100:]), 40)
        hist, _ = np.histogram(close[-100:], bins=price_bins, weights=vol[-100:])
        poc_price = price_bins[np.argmax(hist)]
        above_poc = cp > poc_price
        
        # 2. 口袋枢轴 (Pocket Pivot)
        neg_vol = vol[-11:-1][close[-11:-1] < close[-12:-2]]
        max_neg_vol = np.max(neg_vol) if len(neg_vol) > 0 else 9e12
        is_pocket = (close[-1] > close[-2]) and (vol[-1] > max_neg_vol)

        # 3. VCP 紧致度
        tightness = (np.std(close[-10:]) / np.mean(close[-10:])) * 100

        # --- D. 战法决策系统 ---
        action, score = "观察", 60
        
        if dist_ma50 > 14:
            action, score = "⚠️ 乖離過大", 45
        elif is_pocket and above_poc and tightness < 1.8:
            action, score = "🎯 領袖口袋(Pocket)", 95
        elif rs_nh and cp >= np.max(close[-20:]) and tightness < 2.0:
            action, score = "🚀 巔峰突破(Breakout)", 92
        elif cp > ma50 and tightness < 1.1 and vol[-1] < np.mean(vol[-20:])*0.6:
            action, score = "💎 极致收缩(VCP)", 88
        
        # 环境惩罚
        if not hstech_ok: score -= 15
        if not above_poc: score -= 10

        # --- E. 风险管理 (统帅风控) ---
        adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
        # 止损取 ADR 止损与 MA50 支撑的平衡
        stop_price = max(ma50 * 0.985, cp * (1 - adr_20 * 0.01 * 1.6))
        risk_per_share = cp - stop_price
        shares = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // risk_per_share if risk_per_share > 0 else 0

        return {
            "Action": action, "Score": score, "Price": cp, "Dist_50": round(dist_ma50, 1),
            "Shares": int(shares), "Stop": round(stop_price, 2), "Tight": round(tightness, 2),
            "RS_Turbo": round(rs_turbo, 2), "Turnover_M": round(avg_turnover_20d/1e6, 1),
            "Above_POC": "✅" if above_poc else "-", "rs_long_raw": rs_long,
            "is_stage_2": (cp > ma50 > ma200)
        }
    except: return None

# ==========================================
# 🚀 3. 主程序流程
# ==========================================
def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ 量子统帅旗舰系统启动...")
    
    # 1. 环境审计
    mkt_data = yf.download(["^HSI", "3088.HK"], period="50d", progress=False)['Close']
    hsi_series = mkt_data["^HSI"].dropna()
    hstech = mkt_data["3088.HK"].dropna()
    hstech_ok = hstech.iloc[-1] > hstech.rolling(20).mean().iloc[-1]
    mkt_weather = "☀️ 激进" if hstech_ok else "☁️ 谨慎"

    # 2. 获取票池 (TV扫描前300 + 0700强审)
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "close", "market_cap_basic", "sector"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1.5e10}],
               "range": [0, 300], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        resp = requests.post(url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "其他"} for d in resp])
        tickers = ["0700.HK", "3690.HK", "9988.HK", "1810.HK", "1211.HK", "9888.HK"] # 核心池
        tickers += [c.zfill(4)+".HK" for c in df_pool['code']]
        tickers = list(set(tickers))
    except: return

    # 3. 批量数据审计
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    final_list = []
    
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            res = calculate_quantum_commander(data[t], hsi_series, hstech_ok)
            if res and res['Action'] != "观察" and res['is_stage_2']:
                code_clean = t.split('.')[0].lstrip('0')
                sector = df_pool[df_pool['code']==code_raw]['sector'].iloc[0] if not df_pool[df_pool['code']==code_clean].empty else "领袖"
                res.update({"Ticker": t.split('.')[0], "Sector": sector})
                final_list.append(res)
        except: continue

    if not final_list: 
        print("📭 今日暂无符合顶级形态的信号。"); return

    res_df = pd.DataFrame(final_list)
    # 计算全市场 RS 排名
    res_df['RS_Rank'] = res_df['rs_long_raw'].rank(pct=True).apply(lambda x: int(x*99))
    # 行业限额筛选 (每行业前3)
    res_df = res_df.sort_values(by="Score", ascending=False).groupby("Sector").head(3)

    # 4. 写入 Google Sheets
    sh = init_commander_sheet()
    sh.clear()
    sh.update(range_name="A1", values=[[f"🏯 量子统帅 (Quantum Commander)", f"天气: {mkt_weather}", f"刷新: {now_str}", "组合策略: PocketPivot + POC + RS_Turbo + IndustryCap"]])
    
    cols = ["Ticker", "Action", "RS_Rank", "Score", "Price", "Shares", "Stop", "Tight", "Dist_50", "RS_Turbo", "Sector"]
    sh.update(range_name="A3", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")

    # 5. 条件美化
    set_frozen(sh, rows=3)
    format_cell_range(sh, 'C4:C100', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 1)))) # RS蓝色
    
    rules = get_conditional_format_rules(sh)
    # 乖离率风险高亮
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('I4:I100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['12']),
                                format=cellFormat(backgroundColor=color(1, 0.9, 0.7)))))
    # Pocket Pivot 紫色高亮
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🎯']),
                                format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True)))))
    rules.save()
    
    print(f"✅ 指令已下达！捕捉到 {len(res_df)} 只量子领袖股。")

if __name__ == "__main__":
    main()
