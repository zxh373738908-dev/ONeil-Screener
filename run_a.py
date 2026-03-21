import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging
import yfinance as yf
import akshare as ak

warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 基础设置
# ==========================================
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

def get_worksheet():
    doc = client.open_by_url(OUTPUT_SHEET_URL)
    try:
        return doc.worksheet("A-Share Screener Pro")
    except gspread.exceptions.WorksheetNotFound:
        return doc.add_worksheet(title="A-Share Screener Pro", rows=100, cols=20)

# ==========================================
# 🛠️ 筹码分布确认算法 (TradingView 模拟)
# ==========================================
def check_chip_peak(df, lookback=120):
    """
    计算筹码峰状态
    返回: POC价位, 上方阻力百分比, 状态描述
    """
    if len(df) < lookback: return 0, 1.0, "数据不足"
    
    hist = df.tail(lookback).copy()
    p_min, p_max = hist['Low'].min(), hist['High'].max()
    bins = 50
    price_range = np.linspace(p_min, p_max, bins + 1)
    v_dist = np.zeros(bins)

    for _, row in hist.iterrows():
        # 简化版筹码分配
        idx = np.where((price_range[:-1] >= row['Low']) & (price_range[1:] <= row['High']))[0]
        if len(idx) > 0: v_dist[idx] += row['Volume'] / len(idx)
        else:
            c_idx = np.searchsorted(price_range, row['Close']) - 1
            if 0 <= c_idx < bins: v_dist[c_idx] += row['Volume']

    poc_idx = np.argmax(v_dist)
    poc_price = (price_range[poc_idx] + price_range[poc_idx+1]) / 2
    
    curr_price = df['Close'].iloc[-1]
    curr_idx = np.searchsorted(price_range, curr_price) - 1
    overhead_vol = np.sum(v_dist[curr_idx:]) if curr_idx < bins else 0
    res_ratio = overhead_vol / np.sum(v_dist)

    return round(poc_price, 2), res_ratio, ("真空" if res_ratio < 0.15 else "有压")

# ==========================================
# 🚀 STEP 2: 扫描 (核心逻辑整合)
# ==========================================
def scan_market_with_chips(df_list):
    print("\n🚀 [STEP 2] 启动【动量+筹码】双引擎演算...")
    
    tickers = []
    ticker_to_name = {}
    for _, row in df_list.iterrows():
        c = str(row['code'])
        t = f"{c}.SS" if c.startswith('6') else f"{c}.SZ"
        tickers.append(t)
        ticker_to_name[t] = row['name']
        
    all_results = []
    chunk_size = 500
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"   -> 📥 正在处理第 {i+1} ~ {min(i+chunk_size, len(tickers))} 只标的...")
        data = yf.download(chunk, period="1y", auto_adjust=True, threads=True, progress=False)
        
        for ticker in chunk:
            try:
                # 1. 基础数据准备
                if len(chunk) > 1:
                    df_t = pd.DataFrame({
                        'Open': data['Open'][ticker], 'High': data['High'][ticker],
                        'Low': data['Low'][ticker], 'Close': data['Close'][ticker],
                        'Volume': data['Volume'][ticker]
                    }).dropna()
                else:
                    df_t = data.dropna()
                
                if len(df_t) < 200: continue
                
                closes = df_t['Close'].values
                highs = df_t['High'].values
                lows = df_t['Low'].values
                vols = df_t['Volume'].values
                price = closes[-1]
                
                # -----------------------------------------
                # 🛡️ 您原有的【动量+VCP】筛选逻辑 (完全保留)
                # -----------------------------------------
                turnover_1 = price * vols[-1]
                turnover_5 = np.mean(closes[-5:] * vols[-5:])
                if turnover_5 < 100000000 or price < 5: continue 
                
                # 均线与高点计算
                ma20 = np.mean(closes[-20:]); ma50 = np.mean(closes[-50:])
                ma150 = np.mean(closes[-150:]); ma200 = np.mean(closes[-200:])
                h250 = np.max(highs[-250:])
                
                # 动量计算
                vol_ratio = vols[-1] / np.mean(vols[-50:])
                r20 = (closes[-1] - closes[-21]) / closes[-21]
                r60 = (closes[-1] - closes[-61]) / closes[-61]
                r120 = (closes[-1] - closes[-121]) / closes[-121]
                rs_score = (r20*0.4 + r60*0.3 + r120*0.3) * 100
                dist_high_pct = ((price - h250) / h250) * 100
                
                # VCP 振幅
                amps = (highs[-5:] - lows[-5:]) / lows[-5:] * 100
                avg_amp5 = np.mean(amps)

                # 原有战法判定
                cond_momentum = (rs_score > 80) or (r60 * 100 > 25)
                cond_turnover = 300_000_000 <= turnover_1 <= 1_500_000_000
                
                # [战法A: 引信雷达]
                fuse_radar = (-8 <= dist_high_pct <= -1) and (vol_ratio < 1.1) and (avg_amp5 < 5.0) and cond_momentum
                # [战法B: 狙击触发]
                trigger_sniper = (-8 <= dist_high_pct <= 2) and (vol_ratio > 1.5) and cond_momentum and (price > ma20)
                
                # 基础筛选：必须符合其中一个战法，才进入“筹码峰体检”
                if not (fuse_radar or trigger_sniper):
                    continue

                # -----------------------------------------
                # 👁️ 【方案一：筹码峰二次确认】 (核心新增)
                # -----------------------------------------
                poc, res_ratio, chip_status = check_chip_peak(df_t)
                
                # 筹码确认逻辑：
                # 如果是狙击模式，但上方阻力 > 25%，说明是“强弩之末”，标记风险
                chip_confirmed = "✅ 筹码通透" if res_ratio < 0.2 else "❌ 压力巨大"
                
                # 如果是引信雷达（潜伏），价格最好在 POC 附近
                poc_dist = (price - poc) / poc * 100
                poc_support = "🎯 支撑位" if abs(poc_dist) < 3 else ""

                # 结果组装
                type_label = "🔥 狙击触发" if trigger_sniper else "🧨 引信雷达"
                
                all_results.append({
                    "代码": ticker.split('.')[0],
                    "名称": ticker_to_name[ticker],
                    "现价": round(price, 2),
                    "RS评分": round(rs_score, 1),
                    "战法": type_label,
                    "筹码确认": chip_confirmed,
                    "POC价格": poc,
                    "上方套牢%": f"{round(res_ratio*100, 1)}%",
                    "备注": poc_support,
                    "距高点%": f"{round(dist_high_pct, 1)}%",
                    "成交额(亿)": round(turnover_1 / 100000000, 2)
                })
            except:
                continue
    return all_results

# (后续 get_a_share_list, write_sheet 等函数与之前保持一致)
