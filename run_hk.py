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
# 1. 核心配置 (保持 SS_KEY 和 GID 精准)
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_GID = 665566258  
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_v36_sheet():
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
    chunk_size = 50
    for i in range(0, len(codes), chunk_size):
        chunk = [f"hk{str(c).zfill(5)}" for c in codes[i:i+chunk_size]]
        url = f"http://qt.gtimg.cn/q={','.join(chunk)}"
        try:
            r = requests.get(url, timeout=5)
            matches = re.findall(r'v_hk(\d+)="[^~]+~([^~]+)', r.text)
            for c, n in matches: mapping[str(c).lstrip('0')] = n
        except: pass
    return mapping

# ==========================================
# 🧠 3. V36.0 演算核心：VDU 窒息量 + RS-NH
# ==========================================
def analyze_v36(ticker, name, df_h, mkt_cap, tv_vwap, sector, hsi_series):
    if df_h.empty or len(df_h) < 252: return None
    try:
        close = df_h['Close'].values.flatten()
        high = df_h['High'].values.flatten()
        low = df_h['Low'].values.flatten()
        vol = df_h['Volume'].values.flatten()
        cp = float(close[-1])
    except: return None

    # A. RS 线新高检测 (Relative Strength New High)
    hsi_aligned = hsi_series.reindex(df_h.index).ffill().values
    rs_line = close / hsi_aligned
    rs_nh_120 = rs_line[-1] >= np.max(rs_line[-120:])
    
    # IBD RS 算法
    ret_3m, ret_6m, ret_12m = cp/close[-63], cp/close[-126], cp/close[-252]
    rs_raw = (ret_3m * 0.4 + ret_6m * 0.3 + ret_12m * 0.3) / (hsi_aligned[-1]/hsi_aligned[-252])

    # B. U/D Ratio 与 VDU 窒息量
    diff = np.diff(close[-51:])
    up_v, dn_v = np.sum(vol[-50:][diff > 0]), np.sum(vol[-50:][diff < 0])
    ud_ratio = up_v / dn_v if dn_v > 0.001 else 1.0
    
    avg_vol_50 = np.mean(vol[-50:])
    vdu = vol[-1] < (avg_vol_50 * 0.48) # 窒息量：成交量小于均值的一半

    # C. 口袋突破
    down_vol_max = 0.001
    for i in range(1, 11):
        if close[-i-1] > close[-i]: down_vol_max = max(down_vol_max, vol[-i])
    is_pocket = (close[-1] > close[-2]) and (vol[-1] > down_vol_max)

    # D. 指标与趋势
    ma50, ma200 = np.mean(close[-50:]), np.mean(close[-200:])
    adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
    tightness = (np.max(high[-10:]) - np.min(low[-10:])) / (np.max(high[-50:]) - np.min(low[-50:]) + 0.001)
    
    if cp < ma200: return None # 拒绝牛熊线下
    if adr_20 < 1.4: return None # 拒绝低流动

    # E. 智能勋章
    signal = "📈 经典多头"
    if is_pocket and tightness < 0.6: signal = "🎯 口袋起爆"
    elif vdu and abs(cp - ma50)/ma50 < 0.04: signal = "💎 窒息枯竭"
    elif rs_nh_120 and cp > ma50: signal = "🔵 RS蓝点"
    elif ud_ratio > 1.3: signal = "🐉 机构加仓"

    tr = np.maximum(high - low, np.maximum(abs(high - np.roll(close, 1)), abs(low - np.roll(close, 1))))
    atr = pd.Series(tr).rolling(20).mean().iloc[-1]

    return {
        "Ticker": ticker.replace(".HK", ""), "Name": name, "Action": signal,
        "Sector": sector, "RS_Raw": rs_raw, "UD_Ratio": round(ud_ratio, 2),
        "Tightness": round(tightness, 2), "ADR": round(adr_20, 2),
        "Price": round(cp, 2), "Ext_50": round((cp/ma50-1)*100, 1),
        "Stop_Loss": round(cp - (atr * 2.1), 2), "RS_NH": rs_nh_120,
        "VDU_Flag": vdu
    }

# ==========================================
# 🚀 4. 执行指挥部
# ==========================================
def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V36.0 Alpha 指挥官系统启动...")
    
    hsi_raw = yf.download("^HSI", period="350d", progress=False)
    hsi_series = hsi_raw['Close'].iloc[:, 0] if isinstance(hsi_raw['Close'], pd.DataFrame) else hsi_raw['Close']
    hsi_p, hsi_ma50 = hsi_series.iloc[-1], hsi_series.rolling(50).mean().iloc[-1]
    mkt_weather = "☀️ 进攻 (Bullish)" if hsi_p > hsi_ma50 else "❄️ 防御 (Bearish)"

    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {
        "columns": ["name", "description", "close", "market_cap_basic", "VWAP", "sector"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1.15e10}], # 提高市值门槛
        "range": [0, 400], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    resp = requests.post(url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "mkt": d['d'][3], "sector": d['d'][5] or "其他"} for d in resp])

    name_map = get_chinese_names(df_pool['code'].tolist())
    final_list = []
    tickers = [str(c).zfill(4)+".HK" for c in df_pool['code']]
    chunk_size = 40
    
    for i in range(0, len(tickers), chunk_size):
        batch = tickers[i : i + chunk_size]
        data = yf.download(batch, period="2y", group_by='ticker', progress=False, threads=True)
        for t in batch:
            try:
                code_clean = t.split('.')[0].lstrip('0')
                row = df_pool[df_pool['code'] == code_clean].iloc[0]
                res = analyze_v36(t, name_map.get(code_clean, t), data[t].dropna(), row['mkt'], 0, row['sector'], hsi_series)
                if res: final_list.append(res)
            except: continue
        time.sleep(1.5)

    if not final_list: return
    res_df = pd.DataFrame(final_list)

    # 排名逻辑
    res_df['RS评分'] = res_df['RS_Raw'].rank(pct=True).apply(lambda x: int(x*99))
    sector_scores = res_df.groupby('Sector')['RS评分'].mean().to_dict()
    res_df['Sector_Alpha'] = res_df['Sector'].map(sector_scores).round(1)
    
    # 综合 Commander 分数
    res_df['Score'] = (res_df['RS评分'] * 0.5) + (res_df['UD_Ratio'] * 15) + (res_df['Sector_Alpha'] * 0.2)
    res_df.loc[res_df['RS_NH'], 'Score'] += 15 # RS线新高蓝点加分
    res_df.loc[res_df['VDU_Flag'], 'Score'] += 10 # 窒息量加分

    # 行业暴露管控：每个行业最多取 3 支最强
    res_df = res_df.sort_values(by="Score", ascending=False)
    res_df = res_df.groupby('Sector').head(3)
    
    final_output = res_df[res_df['Ext_50'] < 24].head(50)

    # 推送与美化
    sh = init_v36_sheet()
    sh.clear()
    sh.update(range_name="A1", values=[[f"📊 环境: {mkt_weather}", f"🕒 更新: {now_str}", "💡 战术: 寻找带有 '💎 窒息' 或 '🔵 蓝点' 的行业领袖", "", "", "", "", ""]])
    
    cols = ["Ticker", "Name", "Action", "Sector", "RS评分", "Sector_Alpha", "Price", "Ext_50", "Tightness", "UD_Ratio", "ADR", "Stop_Loss"]
    sh.update(range_name="A3", values=[cols] + final_output[cols].values.tolist(), value_input_option="USER_ENTERED")

    set_frozen(sh, rows=3)
    format_cell_range(sh, 'A3:L3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0.1, 0.1, 0.1)))
    
    rules = get_conditional_format_rules(sh)
    # RS蓝点与窒息量染色
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('C4:C100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['💎']),
                                format=cellFormat(backgroundColor=color(0.8, 0.9, 1), textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0.6))))))
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('C4:C100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🔵']),
                                format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True, foregroundColor=color(0.4, 0, 0.8))))))
    rules.save()
    print(f"✅ V36.0 部署成功。大盘广度: {len(res_df)} 行业已分散。")

if __name__ == "__main__":
    main()
