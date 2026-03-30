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
# 1. 核心配置 (精准锁定)
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_GID = 665566258  
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_v45_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    for ws in doc.worksheets():
        if ws.id == TARGET_GID: return ws
    return doc.get_worksheet(0)

# ==========================================
# 🌐 2. 数据工具
# ==========================================
def get_chinese_names(codes):
    mapping = {}
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
# 🧠 3. V45.0 演算核心：量子哨兵引擎
# ==========================================
def calculate_frvp_poc(highs, lows, vols, lookback=120):
    if len(vols) < lookback: lookback = len(vols)
    h_s, l_s, v_s = highs[-lookback:], lows[-lookback:], vols[-lookback:]
    min_p, max_p = np.min(l_s), np.max(h_s)
    if max_p == min_p: return min_p
    bins = 50
    bin_size = (max_p - min_p) / bins
    profile = np.zeros(bins)
    for i in range(lookback):
        s_bin = max(0, int((l_s[i] - min_p) / bin_size))
        e_bin = min(bins - 1, int((h_s[i] - min_p) / bin_size))
        profile[s_bin:e_bin+1] += v_s[i] / (e_bin - s_bin + 1)
    return min_p + (np.argmax(profile) + 0.5) * bin_size

def analyze_v45(ticker, name, df_h, mkt_cap, sector, hsi_series):
    if df_h.empty or len(df_h) < 252: return None
    try:
        close = df_h['Close'].values.flatten()
        high = df_h['High'].values.flatten()
        low = df_h['Low'].values.flatten()
        vol = df_h['Volume'].values.flatten()
        cp = float(close[-1])
    except: return None

    # --- A. 动能活跃度过滤 ---
    ma50, ma200 = np.mean(close[-50:]), np.mean(close[-200:])
    avg_vol50 = np.mean(vol[-50:])
    vol_ratio = vol[-1] / avg_vol50
    adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
    if adr_20 < 1.8 or cp < ma200 * 0.8: return None # 彻底拒绝死鱼和深跌股

    # --- B. 滞涨检测 (Churn Detection) ---
    # 规则：放量(1.5倍)但涨幅小于0.5%
    is_churn = (vol_ratio > 1.5) and (abs(cp/close[-2]-1) < 0.005)

    # --- C. 相对强度斜率 (RS Velocity) ---
    hsi_aligned = hsi_series.reindex(df_h.index).ffill().values
    rs_line = close / hsi_aligned
    rs_slope = (rs_line[-1] - rs_line[-10]) / (rs_line[-10] + 0.001) * 100
    
    # --- D. 机构足迹计数 ---
    acc_days = 0
    for i in range(1, 21):
        if close[-i] > close[-i-1] and vol[-i] > vol[-i-1]: acc_days += 1

    osi = (np.max(high[-252:]) / cp - 1) * 100
    poc = calculate_frvp_poc(high, low, vol)

    # --- E. 战法决策系统 ---
    action = "📈 趨勢持有"
    prio = 0
    is_mega = mkt_cap > 2000e8
    vdu = vol[-1] < avg_vol50 * 0.5 # 窒息量

    if is_mega and rs_line[-1] >= np.max(rs_line[-60:]) and rs_slope > 0:
        action = "🛰️ 巨頭起航"
        prio = 95
    elif cp > np.max(close[-20:]) and vol_ratio > 1.8 and osi > 8:
        action = "🚀 動能噴發"
        prio = 90
    elif abs(cp - ma50)/ma50 < 0.03 and (vdu or acc_days > 8):
        action = "🐉 老龍回踩"
        prio = 85
    elif cp < ma200 and cp > np.mean(close[-20:]) and vol_ratio > 1.6 and cp > poc:
        action = "🐣 鳳凰涅槃"
        prio = 75
        
    if is_churn: action = "⚠️ 高位滯漲"; prio -= 20

    # ATR 止损 (V45 trailing优化)
    tr = np.maximum(high - low, np.maximum(abs(high - np.roll(close, 1)), abs(low - np.roll(close, 1))))
    atr = pd.Series(tr).rolling(20).mean().iloc[-1]
    stop_loss = cp - (atr * (1.7 if is_mega else 2.1))

    # IBD RS 分计算
    ret_3m, ret_6m, ret_12m = cp/close[-63], cp/close[-126], cp/close[-252]
    rs_raw = (ret_3m * 0.4 + ret_6m * 0.3 + ret_12m * 0.3) / (hsi_aligned[-1]/hsi_aligned[-252])

    return {
        "Ticker": ticker.replace(".HK", ""), "Name": name, "Action": action,
        "Price": round(cp, 2), "RS_Rank": 0, "ADR": round(adr_20, 2),
        "RS_Vel": round(rs_slope, 2), "Acc_Days": acc_days, "OSI": round(osi, 1),
        "VDU": "💎" if vdu else "", "Stop_Loss": round(stop_loss, 2),
        "Sector": sector, "rs_raw": rs_raw, "prio": prio, "is_bull": cp > ma200
    }

# ==========================================
# 🚀 4. 执行流程
# ==========================================
def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V45.0 量子哨兵系统启动...")
    
    # 1. 指数对冲
    hsi_raw = yf.download("^HSI", period="350d", progress=False)
    hsi_series = hsi_raw['Close'].iloc[:, 0] if isinstance(hsi_raw['Close'], pd.DataFrame) else hsi_raw['Close']
    hsi_p, hsi_ma50 = hsi_series.iloc[-1], hsi_series.rolling(50).mean().iloc[-1]
    
    # 2. 获取初筛池
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "description", "close", "market_cap_basic", "sector"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1e10}],
               "range": [0, 450], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    resp = requests.post(url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "mkt": d['d'][3], "sector": d['d'][4] or "其他"} for d in resp])

    name_map = get_chinese_names(df_pool['code'].tolist())
    final_list = []
    tickers = [str(c).zfill(4)+".HK" for c in df_pool['code']]
    
    for i in range(0, len(tickers), 40):
        chunk = tickers[i : i + 40]
        data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
        for t in chunk:
            try:
                code_clean = t.split('.')[0].lstrip('0')
                res = analyze_v45(t, name_map.get(code_clean, t), data[t].dropna(), 
                                 df_pool[df_pool['code']==code_clean].iloc[0]['mkt'], 
                                 df_pool[df_pool['code']==code_clean].iloc[0]['sector'], hsi_series)
                if res: final_list.append(res)
            except: continue
        time.sleep(1)

    if not final_list: return
    res_df = pd.DataFrame(final_list)

    # 3. 量子评分排名
    res_df['RS_Rank'] = res_df['rs_raw'].rank(pct=True).apply(lambda x: int(x*99))
    # 行业热度共振
    sector_heat = res_df['Sector'].value_counts().to_dict()
    res_df['Sector_Heat'] = res_df['Sector'].map(sector_heat)
    
    # 量子总分 = 战法基础分 + RS*0.4 + 行业热度*2 + 机构足迹*2 + RS速度
    res_df['Score'] = res_df['prio'] + (res_df['RS_Rank'] * 0.4) + (res_df['Sector_Heat'] * 2) + (res_df['Acc_Days'] * 1.5) + (res_df['RS_Vel'] * 2)
    
    # 评级 AAA/AA/A
    res_df['Rating'] = res_df['Score'].apply(lambda x: "AAA" if x > 118 else ("AA" if x > 98 else "A"))
    final_output = res_df.sort_values(by="Score", ascending=False).groupby('Sector').head(3).head(65)

    # 4. 写入与美化
    sh = init_v45_sheet()
    sh.clear()
    bull_pct = int((res_df['is_bull'].sum() / 450) * 100)
    mkt_weather = "☀️ 激进" if hsi_p > hsi_ma50 else "❄️ 观望"
    
    sh.update(range_name="A1", values=[[f"📡 量子哨兵: {mkt_weather}", f"📈 廣度: {bull_pct}%", f"🕒 刷新: {now_str}", "評级: AAA-極品 | AA-優選 | A-關注", "", "", "", "", "", ""]])
    
    cols = ["Ticker", "Name", "Rating", "Action", "RS_Rank", "RS_Vel", "Price", "VDU", "Acc_Days", "Sector_Heat", "Stop_Loss"]
    sh.update(range_name="A3", values=[cols] + final_output[cols].values.tolist(), value_input_option="USER_ENTERED")

    # 美化
    set_frozen(sh, rows=3)
    format_cell_range(sh, 'A3:K3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0, 0, 0)))
    
    rules = get_conditional_format_rules(sh)
    # AAA 红色
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('C4:C100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['AAA']),
                                format=cellFormat(backgroundColor=color(1, 0.9, 0.6), textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0, 0))))))
    # 滞涨橙色警告
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('D4:D100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['⚠️']),
                                format=cellFormat(backgroundColor=color(1, 0.8, 0.6), textFormat=textFormat(bold=True)))))
    rules.save()
    print(f"✅ V45.0 任务完成。当前板块热度王: {res_df.groupby('Sector')['Sector_Heat'].max().idxmax()}")

if __name__ == "__main__":
    main()
