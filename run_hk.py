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
        # 数据完整性检查
        df = df.dropna(subset=['Close'])
        if len(df) < 150: return "DATA_SHORT"
        
        close = df['Close'].values.astype(float)
        high = df['High'].values.astype(float)
        low = df['Low'].values.astype(float)
        vol = df['Volume'].values.astype(float)
        turnover = close * vol
        cp = close[-1]
        
        # --- A. 基础门槛 (调整门槛以防列表为空) ---
        avg_turn_20 = np.mean(turnover[-20:])
        if avg_turn_20 < 60000000: return "LOW_LIQUID" # 降至6000万用于测试

        ma50 = np.mean(close[-50:])
        dist_ma50 = (cp / ma50 - 1) * 100
        
        # --- B. 核心组件 ---
        # RS线对齐
        common_idx = hsi_series.index.intersection(df.index)
        if len(common_idx) < 50: return "RS_FAILED"
        rs_line = close[df.index.get_indexer(common_idx)] / hsi_series.loc[common_idx].values
        rs_nh = rs_line[-1] >= np.max(rs_line[-250:]) 
        rs_turbo = (rs_line[-1] / rs_line[-6] - 1) * 100 if len(rs_line) > 6 else 0

        # POC 筹码
        price_bins = np.linspace(np.min(low[-100:]), np.max(high[-100:]), 40)
        hist, _ = np.histogram(close[-100:], bins=price_bins, weights=vol[-100:])
        poc_price = price_bins[np.argmax(hist)]
        
        # 紧致度
        tightness = (np.std(close[-10:]) / np.mean(close[-10:])) * 100
        vdu = vol[-1] < np.mean(vol[-20:]) * 0.65

        # --- C. 四战法判定 ---
        signals = []
        score = 60

        if rs_nh and cp < np.max(close[-20:]) * 1.02: signals.append("奇點"); score += 15
        if cp > ma50 and dist_ma50 < 4.0 and (vdu or tightness < 1.3): signals.append("老龍"); score += 15
        if cp >= np.max(close[-20:]) and vol[-1] > np.mean(vol[-20:]) * 1.2: signals.append("突破"); score += 15
        if rs_nh and hstech_ok and cp > poc_price: signals.append("共振"); score += 15

        if not signals: return "NO_SIGNAL"
        
        # 统帅判定
        if len(signals) >= 3: final_action = "💎 統帥共振(SUPER)"; score += 25
        elif len(signals) == 2: final_action = f"🔥 双重({'+'.join(signals)})"; score += 10
        else:
            action_map = {"奇點":"👁️ 奇點先行", "老龍":"🐉 老龍回頭", "突破":"🚀 巔峰突破", "共振":"🌟 雙重共振"}
            final_action = action_map[signals[0]]

        if dist_ma50 > 15: final_action = "⚠️ 乖離過大"; score = 40

        # --- D. 风险头寸 ---
        adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
        stop_p = max(ma50 * 0.985, cp * (1 - adr_20 * 0.01 * 1.6))
        risk_per = cp - stop_p
        shares = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // risk_per if risk_per > 0 else 0

        return {
            "Action": final_action, "Score": score, "Price": cp, "Dist_50": round(dist_ma50, 1),
            "Shares": int(shares), "Stop": round(stop_p, 2), "Tight": round(tightness, 2),
            "RS_Turbo": round(rs_turbo, 2), "Above_POC": "✅" if cp > poc_price else "-",
            "is_stage_2": cp > ma50, "rs_raw": cp/close[-120] if len(close)>120 else 1
        }
    except: return "ERROR"

# ==========================================
# 🚀 3. 主程序流程
# ==========================================
def main():
    start_time = time.time()
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%m-%d %H:%M')
    print(f"[{now_str}] 🛰️ V45 量子统帅 [终极版] 启动...")
    
    sh = init_commander_sheet()
    sh.update(range_name="A1", values=[[f"🏯 正在执行量子审计...", f"心跳: {now_str}", "状态: 数据抓取中..."]])

    # 1. 环境审计
    mkt_data = yf.download(["^HSI", "3088.HK"], period="60d", progress=False)['Close']
    hsi_series = mkt_data["^HSI"].dropna()
    hstech = mkt_data["3088.HK"].dropna()
    hstech_ok = hstech.iloc[-1] > hstech.rolling(20).mean().iloc[-1] if not hstech.empty else False
    mkt_weather = "☀️ 激进" if hstech_ok else "☁️ 谨慎"

    # 2. 票池初筛
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "close", "market_cap_basic", "sector"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1e10}],
               "range": [0, 300], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    
    resp = requests.post(url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "其他"} for d in resp])
    all_codes = list(set(df_pool['code'].tolist() + ["0700", "3690", "9988", "1810"]))
    tickers = [c.zfill(4)+".HK" for c in all_codes]
    name_map = get_chinese_names(all_codes)

    # 3. 数据审计
    print(f"🔎 正在审计 {len(tickers)} 只标的...")
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    final_list = []
    stats = {"LOW_LIQUID": 0, "NO_SIGNAL": 0, "DATA_SHORT": 0, "SUCCESS": 0}

    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            res = calculate_commander_signals(data[t], hsi_series, hstech_ok)
            
            if isinstance(res, str):
                if res in stats: stats[res] += 1
                continue
            
            if res and res['is_stage_2']:
                code_clean = t.split('.')[0].lstrip('0')
                sector_search = df_pool[df_pool['code'] == code_clean]
                sector = sector_search['sector'].iloc[0] if not sector_search.empty else "領袖"
                res.update({"Ticker": t.split('.')[0], "Name": name_map.get(code_clean, t), "Sector": sector})
                final_list.append(res)
                stats["SUCCESS"] += 1
        except: continue

    print(f"📊 审计统计: {stats}")

    # 4. 写入 Google Sheets
    sh.clear()
    sh.update(range_name="A1", values=[[f"🏯 V45 量子统帅旗舰版", f"环境: {mkt_weather}", f"刷新时间: {now_str}", f"有效信号: {len(final_list)}"]])
    
    if final_list:
        res_df = pd.DataFrame(final_list)
        res_df['RS_Rank'] = res_df['rs_raw'].rank(pct=True).apply(lambda x: int(x*99))
        res_df = res_df.sort_values(by="Score", ascending=False).groupby("Sector").head(3)
        
        cols = ["Ticker", "Name", "Action", "Score", "RS_Rank", "Price", "Shares", "Stop", "Tight", "Dist_50", "Sector"]
        sh.update(range_name="A3", values=[cols] + res_df[cols].values.tolist(), value_input_option="USER_ENTERED")
        
        # 美化格式
        set_frozen(sh, rows=3)
        format_cell_range(sh, 'A3:K3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0,0,0)))
        print(f"✅ 表格同步成功。")
    else:
        msg = "📭 今日暂无符合形态的标的。原因: 1.成交额低于6000万 2.无收缩形态"
        sh.update_acell("A4", msg)
        print(msg)

    print(f"🏁 运行耗时: {round(time.time() - start_time, 1)}秒")

if __name__ == "__main__":
    main()
