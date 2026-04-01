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
# 1. 配置中心 (请务必核对您的 SS_KEY 和 GID)
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

def get_chinese_names(codes):
    mapping = {}
    if not codes: return mapping
    for i in range(0, len(codes), 50):
        chunk = [f"hk{str(c).zfill(5)}" for c in codes[i:i+50]]
        url = f"http://qt.gtimg.cn/q={','.join(chunk)}"
        try:
            r = requests.get(url, timeout=5)
            matches = re.findall(r'v_hk(\d+)="[^~]+~([^~]+)', r.text)
            for c, n in matches: mapping[str(c).lstrip('0')] = n
        except: pass
    return mapping

# ==========================================
# 🧠 2. 量子统帅[四剑合一]引擎
# ==========================================
def calculate_commander_signals(df, hsi_series, hstech_ok):
    try:
        df = df.dropna(subset=['Close'])
        if len(df) < 180: return None
        
        close = df['Close'].values.astype(float)
        high = df['High'].values.astype(float)
        low = df['Low'].values.astype(float)
        vol = df['Volume'].values.astype(float)
        turnover = close * vol
        cp = close[-1]
        
        # --- A. 基础门槛 (流动性 + 均线) ---
        avg_turn_20 = np.mean(turnover[-20:])
        if avg_turn_20 < 80000000: return "LOW_LIQUID" # 8000万门槛

        ma50 = np.mean(close[-50:])
        ma200 = np.mean(close[-200:])
        dist_ma50 = (cp / ma50 - 1) * 100
        
        # --- B. 核心组件计算 ---
        # 1. RS线与RS新高
        common_idx = hsi_series.index.intersection(df.index)
        rs_line = close[df.index.get_indexer(common_idx)] / hsi_series.loc[common_idx].values
        rs_nh = rs_line[-1] >= np.max(rs_line[-250:]) # 1年RS新高
        rs_turbo = (rs_line[-1] / rs_line[-6] - 1) * 100 # 短线RS提速

        # 2. POC 筹码中心 (100日)
        price_bins = np.linspace(np.min(low[-100:]), np.max(high[-100:]), 40)
        hist, _ = np.histogram(close[-100:], bins=price_bins, weights=vol[-100:])
        poc_price = price_bins[np.argmax(hist)]
        
        # 3. 紧致度与量能
        tightness = (np.std(close[-10:]) / np.mean(close[-10:])) * 100
        neg_vol = vol[-11:-1][close[-11:-1] < close[-12:-2]]
        max_neg_vol = np.max(neg_vol) if len(neg_vol) > 0 else 9e12
        vdu = vol[-1] < np.mean(vol[-20:]) * 0.6

        # --- C. 四战法矩阵判定 ---
        signals = []
        score = 60

        # 1. 奇點先行 (Stealth): RS新高但股价在低位盘整
        if rs_nh and cp < np.max(close[-20:]) * 1.02:
            signals.append("奇點")
            score += 15
        
        # 2. 老龍回頭 (Pullback): 强势股回踩MA50且缩量
        if cp > ma50 and dist_ma50 < 3.5 and (vdu or tightness < 1.2):
            signals.append("老龍")
            score += 15
            
        # 3. 巔峰突破 (Breakout): 放量创20日新高
        if cp >= np.max(close[-20:]) and vol[-1] > max_neg_vol and vol[-1] > np.mean(vol[-20:]) * 1.3:
            signals.append("突破")
            score += 15
            
        # 4. 雙重共振 (Resonance): 个股强、板块强、且过POC位
        if rs_nh and hstech_ok and cp > poc_price:
            signals.append("共振")
            score += 15

        # --- D. 统帅共振逻辑 ---
        if not signals: return None
        
        if len(signals) >= 3:
            final_action = "💎 統帥共振(SUPER)"
            score += 25
        elif len(signals) == 2:
            final_action = f"🔥 双重({'+'.join(signals)})"
            score += 10
        else:
            action_map = {"奇點":"👁️ 奇點先行", "老龍":"🐉 老龍回頭", "突破":"🚀 巔峰突破", "共振":"🌟 雙重共振"}
            final_action = action_map[signals[0]]

        if dist_ma50 > 14: final_action = "⚠️ 乖離過大"; score = 40

        # --- E. 风险头寸 ---
        adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
        stop_price = max(ma50 * 0.985, cp * (1 - adr_20 * 0.01 * 1.6))
        risk_per_share = cp - stop_price
        shares = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // risk_per_share if risk_per_share > 0 else 0

        return {
            "Action": final_action, "Score": score, "Price": cp, "Dist_50": round(dist_ma50, 1),
            "Shares": int(shares), "Stop": round(stop_price, 2), "Tight": round(tightness, 2),
            "RS_Turbo": round(rs_turbo, 2), "Above_POC": "✅" if cp > poc_price else "-",
            "is_stage_2": (cp > ma50), "rs_raw": cp/close[-120] if len(close)>120 else 1
        }
    except: return None

# ==========================================
# 🚀 3. 主程序流程
# ==========================================
def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ V45 量子统帅[四剑合一版]启动...")
    
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

    # 更新心跳，确保你知道程序在跑
    sh.update(range_name="A1", values=[[f"🏯 量子统帅正在审计...", f"环境: {mkt_weather}", f"心跳时间: {now_str}", "状态: 正在下载个股数据..."]])

    # 2. 获取初筛池 (TV扫描 + 核心股审计)
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "close", "market_cap_basic", "sector"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1.2e10}],
               "range": [0, 300], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    
    try:
        resp = requests.post(url, json=payload, timeout=15).json().get('data', [])
        df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "其他"} for d in resp])
        # 强制审计领袖股
        leaders = ["0700", "3690", "9988", "1810", "1211", "9888", "2318", "0941"]
        all_codes = list(set(df_pool['code'].tolist() + leaders))
        tickers = [c.zfill(4)+".HK" for c in all_codes]
        name_map = get_chinese_names(all_codes)
    except: return

    # 3. 批量执行审计
    print(f"🔎 正在对 {len(tickers)} 只个股执行[四剑合一]形态审计...")
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    final_list = []
    low_liq_count = 0
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            res = calculate_commander_signals(data[t], hsi_series, hstech_ok)
            
            if res == "LOW_LIQUID": low_liq_count += 1; continue
            
            if res and res['is_stage_2']:
                code_clean = t.split('.')[0].lstrip('0')
                sector_search = df_pool[df_pool['code'] == code_raw if 'code_raw' in locals() else code_clean]
                sector = sector_search['sector'].iloc[0] if not sector_search.empty else "領袖"
                res.update({"Ticker": t.split('.')[0], "Name": name_map.get(code_clean, t), "Sector": sector})
                final_list.append(res)
        except: continue

    print(f"📊 审计结束：{len(final_list)} 只符合信号，{low_liq_count} 只因成交额不足被剔除。")

    # 4. 写入 Google Sheets
    sh.clear()
    sh.update(range_name="A1", values=[[f"🏯 V45 量子统帅旗舰看板", f"环境: {mkt_weather}", f"刷新时间: {now_str}", f"有效信号: {len(final_list)}"]])
    
    if final_list:
        res_df = pd.DataFrame(final_list)
        res_df['RS_Rank'] = res_df['rs_raw'].rank(pct=True).apply(lambda x: int(x*99))
        # 行业限额筛选：每个行业取前3名
        res_df = res_df.sort_values(by="Score", ascending=False).groupby("Sector").head(3)
        
        cols = ["Ticker", "Name", "Action", "Score", "RS_Rank", "Price", "Shares", "Stop", "Tight", "Dist_50", "Sector"]
        sh.update(range_name="A3", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
        
        # --- 美化与高亮 ---
        set_frozen(sh, rows=3)
        format_cell_range(sh, 'A3:K3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0,0,0)))
        
        rules = get_conditional_format_rules(sh)
        # 統帥共振 - 紫色高亮
        rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('C4:C100', sh)],
            booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['💎']),
                                    format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True)))))
        # 奇點先行 - 蓝色背景
        rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('C4:C100', sh)],
            booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['👁️']),
                                    format=cellFormat(backgroundColor=color(0.8, 0.9, 1)))))
        rules.save()
        print(f"✅ Google Sheet 统帅看板同步成功。")
    else:
        sh.update_acell("A4", "📭 今日暂无符合[四剑合一]顶级形态的标的。建议关注环境天气。")
        print(f"📭 审计完成，无符合信号。")

if __name__ == "__main__":
    main()
