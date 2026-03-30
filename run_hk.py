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
# 1. 核心配置
# ==========================================
SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_GID = 665566258  
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_v39_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    for ws in doc.worksheets():
        if ws.id == TARGET_GID: return ws
    return doc.get_worksheet(0)

# ==========================================
# 🌐 2. 增强型数据工具
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
# 🧠 3. V39.0 演算核心：堡垒指挥官
# ==========================================
def analyze_v39(ticker, name, df_h, mkt_cap, sector, hsi_series):
    if df_h.empty or len(df_h) < 252: return None
    try:
        close = df_h['Close'].values.flatten()
        high = df_h['High'].values.flatten()
        low = df_h['Low'].values.flatten()
        vol = df_h['Volume'].values.flatten()
        cp = float(close[-1])
    except: return None

    # A. 趋势与广度基础
    ma50, ma200 = np.mean(close[-50:]), np.mean(close[-200:])
    if cp < ma200: return None # 铁律 1: 必须在牛熊线上方

    # B. 机构 A/D 统计 (近 25 天)
    # 规则：成交量大于昨日且上涨 = 吸筹(+1)；成交量大于昨日且下跌 = 派发(-1)
    ad_score = 0
    for i in range(1, 26):
        if vol[-i] > vol[-i-1]:
            if close[-i] > close[-i-1]: ad_score += 1
            elif close[-i] < close[-i-1]: ad_score -= 1

    # C. 相对强度新高检测 (Blue Dot)
    hsi_aligned = hsi_series.reindex(df_h.index).ffill().values
    rs_line = close / hsi_aligned
    rs_nh_252 = rs_line[-1] >= np.max(rs_line[-252:])
    rs_slope_10 = (rs_line[-1] - rs_line[-10]) / (rs_line[-10] + 0.001)

    # D. 紧致度与窒息量 (VDU)
    range_10 = np.max(high[-10:]) - np.min(low[-10:])
    range_50 = np.max(high[-50:]) - np.min(low[-50:])
    tightness = range_10 / (range_50 + 0.001)
    vdu = vol[-1] < (np.mean(vol[-50:]) * 0.45) # 极度萎缩

    # E. 智能决策系统
    is_mega = mkt_cap > 2500e8
    signal = "📈 稳健多头"
    
    if rs_nh_252 and ad_score >= 3:
        signal = "👑 极品主升 (RS新高)"
    elif is_mega and rs_slope_10 > 0 and rs_line[-1] > np.mean(rs_line[-20:]):
        signal = "🌊 巨头起步 (Sentinel)"
    elif tightness < 0.35 and vdu:
        signal = "💎 窒息埋伏 (VCP)"
    elif close[-1] > close[-2] and vol[-1] > vol[-2] and vol[-1] > np.mean(vol[-5:]):
        signal = "🎯 动能起爆"

    # F. ATR 动态止损 (优化版)
    tr = np.maximum(high - low, np.maximum(abs(high - np.roll(close, 1)), abs(low - np.roll(close, 1))))
    atr = pd.Series(tr).rolling(20).mean().iloc[-1]
    stop_mult = 1.7 if is_mega else 2.1
    stop_loss = cp - (atr * stop_mult)

    # RS 原始分 (IBD 3/6/12 权重)
    ret_3m, ret_6m, ret_12m = cp/close[-63], cp/close[-126], cp/close[-252]
    rs_raw = (ret_3m * 0.4 + ret_6m * 0.3 + ret_12m * 0.3) / (hsi_aligned[-1]/hsi_aligned[-252])

    return {
        "Ticker": ticker.replace(".HK", ""), "Name": name, "Action": signal,
        "Sector": sector, "RS_Raw": rs_raw, "AD_Score": ad_score,
        "Price": round(cp, 2), "Ext_50": round((cp/ma50-1)*100, 1),
        "Tightness": round(tightness, 2), "Mkt_Cap": round(mkt_cap/1e8, 1),
        "Stop_Loss": round(stop_loss, 2), "RS_NH": rs_nh_252, "in_bull": cp > ma200
    }

# ==========================================
# 🚀 4. 执行指挥部
# ==========================================
def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V39.0 堡垒指挥官系统启动...")
    
    # 获取恒指
    hsi_raw = yf.download("^HSI", period="350d", progress=False)
    hsi_series = hsi_raw['Close'].iloc[:, 0] if isinstance(hsi_raw['Close'], pd.DataFrame) else hsi_raw['Close']
    hsi_ma200 = hsi_series.rolling(200).mean().iloc[-1]
    
    # 获取蓝筹池
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {
        "columns": ["name", "description", "close", "market_cap_basic", "sector"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1.1e10}],
        "range": [0, 450], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    resp = requests.post(url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "mkt": d['d'][3], "sector": d['d'][4] or "其他"} for d in resp])

    name_map = get_chinese_names(df_pool['code'].tolist())
    final_list = []
    tickers = [str(c).zfill(4)+".HK" for c in df_pool['code']]
    
    # 分批演算
    for i in range(0, len(tickers), 40):
        chunk = tickers[i : i + 40]
        data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
        for t in chunk:
            try:
                code_clean = t.split('.')[0].lstrip('0')
                row = df_pool[df_pool['code'] == code_clean].iloc[0]
                res = analyze_v39(t, name_map.get(code_clean, t), data[t].dropna(), row['mkt'], row['sector'], hsi_series)
                if res: final_list.append(res)
            except: continue
        time.sleep(1)

    if not final_list: return
    res_df = pd.DataFrame(final_list)

    # 1. 计算市场广度
    total_scanned = len(df_pool)
    bull_count = res_df['in_bull'].sum()
    breadth_pct = (bull_count / total_scanned) * 100
    if breadth_pct > 60: mood = "🔥 全面进攻 (Risk-On)"
    elif breadth_pct > 40: mood = "⛅ 局部机会 (Selective)"
    else: mood = "❄️ 现金为王 (Defensive)"

    # 2. 深度评分：RS(50%) + A/D(30%) + 紧致奖励(20%)
    res_df['RS评分'] = res_df['RS_Raw'].rank(pct=True).apply(lambda x: int(x*99))
    res_df['Score'] = (res_df['RS评分'] * 0.5) + (res_df['AD_Score'] * 5) + (1 - res_df['Tightness']) * 20
    res_df.loc[res_df['RS_NH'], 'Score'] += 20 # 蓝点加权

    # 3. 排序与去重
    final_output = res_df.sort_values(by="Score", ascending=False).groupby('Sector').head(3)
    final_output = final_output[final_output['Ext_50'] < 25].head(60)

    # 4. 写入表格
    sh = init_v39_sheet()
    sh.clear()
    header_info = [[f"🏗️ 堡垒状态: {mood}", f"📈 广度: {int(breadth_pct)}%", f"🕒 刷新: {now_str}", "💡 指令: 寻找 '👑' 或 '💎'，广度低时严格控制总仓位", "", "", "", "", "", ""]]
    sh.update(range_name="A1", values=header_info)
    
    cols = ["Ticker", "Name", "Action", "RS评分", "AD_Score", "Price", "Ext_50", "Tightness", "Mkt_Cap", "Stop_Loss"]
    sh.update(range_name="A3", values=[cols] + final_output[cols].values.tolist(), value_input_option="USER_ENTERED")

    # 视觉美化
    set_frozen(sh, rows=3)
    format_cell_range(sh, 'A3:J3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0, 0, 0)))
    
    rules = get_conditional_format_rules(sh)
    # 极品主升紫色标记
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('C4:C100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['👑']),
                                format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True, foregroundColor=color(0.4, 0, 0.8))))))
    # 窒息埋伏蓝色标记
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('C4:C100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['💎']),
                                format=cellFormat(backgroundColor=color(0.8, 0.9, 1), textFormat=textFormat(bold=True, foregroundColor=color(0, 0.4, 0.8))))))
    rules.save()
    print(f"✅ V39.0 堡垒指挥官推送成功！当前广度: {int(breadth_pct)}%")

if __name__ == "__main__":
    main()
