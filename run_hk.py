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

def init_v35_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SS_KEY)
    for ws in doc.worksheets():
        if ws.id == TARGET_GID:
            return ws
    return doc.get_worksheet(0)

# ==========================================
# 🌐 2. 增强型数据工具
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
# 🧠 3. V35 核心算法：吸筹比与口袋突破
# ==========================================
def analyze_v35(ticker, name, df_h, mkt_cap, turnover, tv_vwap, sector, hsi_series):
    if df_h.empty or len(df_h) < 250: return None
    try:
        close = df_h['Close'].values.flatten()
        high = df_h['High'].values.flatten()
        low = df_h['Low'].values.flatten()
        vol = df_h['Volume'].values.flatten()
        cp = float(close[-1])
    except: return None

    # A. IBD 加权 RS 算法 (3/6/12个月)
    ret_3m = cp / close[-63] if len(close) > 63 else 1
    ret_6m = cp / close[-126] if len(close) > 126 else 1
    ret_12m = cp / close[-252] if len(close) > 252 else 1
    # 按照 4:3:3 权重比例
    hsi_ret = hsi_series.iloc[-1] / hsi_series.iloc[-252]
    rs_raw = (ret_3m * 0.4 + ret_6m * 0.3 + ret_12m * 0.3) / hsi_ret

    # B. U/D Volume Ratio (50D) - 机构吸筹活跃度
    # 统计过去50天：上涨日成交量之和 / 下跌日成交量之和
    diff = np.diff(close[-51:])
    up_v = np.sum(vol[-50:][diff > 0])
    dn_v = np.sum(vol[-50:][diff < 0])
    ud_ratio = up_v / dn_v if dn_v > 0 else 1.0

    # C. Pocket Pivot (口袋突破) 检测
    # 规则：今日上涨 + 成交量 > 过去10天内最大的那个阴线量
    down_vol_max = 0
    for i in range(1, 11):
        if close[-i-1] > close[-i]: # 阴线
            down_vol_max = max(down_vol_max, vol[-i])
    is_pocket = (close[-1] > close[-2]) and (vol[-1] > down_vol_max)

    # D. 基础指标与风控
    ma10, ma50, ma200 = np.mean(close[-10:]), np.mean(close[-50:]), np.mean(close[-200:])
    adr_20 = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
    tightness = (np.max(high[-10:]) - np.min(low[-10:])) / (np.max(high[-50:]) - np.min(low[-50:]))
    ext_50 = (cp / ma50 - 1) * 100
    
    if cp < ma200: return None # 拒绝空头
    if adr_20 < 1.4: return None # 拒绝死鱼

    # E. 智能勋章系统
    signal = "📈 经典多头"
    if is_pocket and tightness < 0.65: signal = "🎯 口袋起爆"
    elif abs(cp - ma50)/ma50 < 0.03 and ud_ratio > 1.2: signal = "🐉 老龙吸筹"
    elif cp > np.max(close[-20:]) and vol[-1] > np.mean(vol[-50:]) * 1.5: signal = "🚀 动能突围"

    # F. ATR 动态止损位 (2.1倍ATR)
    tr = np.maximum(high - low, np.maximum(abs(high - np.roll(close, 1)), abs(low - np.roll(close, 1))))
    atr = pd.Series(tr).rolling(20).mean().iloc[-1]

    return {
        "Ticker": ticker.replace(".HK", ""), "Name": name, "Action": signal,
        "Sector": sector, "RS_Raw": rs_raw, "UD_Ratio": round(ud_ratio, 2),
        "Tightness": round(tightness, 2), "ADR": round(adr_20, 2),
        "Price": round(cp, 2), "Ext_50": round(ext_50, 1),
        "Stop_Loss": round(cp - (atr * 2.1), 2),
        "Mkt_Cap": round(float(mkt_cap/1e8), 1), "Score_Sort": rs_raw
    }

# ==========================================
# 🚀 4. 执行指挥部
# ==========================================
def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V35.0 机构吸筹猎手启动...")
    
    # 1. 宏观对冲背景
    hsi_raw = yf.download("^HSI", period="350d", progress=False)
    hsi_series = hsi_raw['Close'].iloc[:, 0] if isinstance(hsi_raw['Close'], pd.DataFrame) else hsi_raw['Close']
    hsi_p = hsi_series.iloc[-1]
    hsi_ma50 = hsi_series.rolling(50).mean().iloc[-1]
    mkt_weather = "☀️ 进攻 (Risk-On)" if hsi_p > hsi_ma50 else "❄️ 防御 (Risk-Off)"

    # 2. 获取蓝筹池
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {
        "columns": ["name", "description", "close", "market_cap_basic", "VWAP", "Value.Traded", "sector"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1.1e10}],
        "range": [0, 400], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    resp = requests.post(url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([
        {"code": re.sub(r'[^0-9]', '', d['d'][0]), "mkt": d['d'][3], 
         "vwap": d['d'][4], "turnover": d['d'][5], "sector": d['d'][6] or "其他"} for d in resp
    ])

    name_map = get_chinese_names(df_pool['code'].tolist())

    # 3. 批量演算
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
                res = analyze_v35(t, name_map.get(code_clean, t), data[t].dropna(), row['mkt'], row['turnover'], row['vwap'], row['sector'], hsi_series)
                if res: final_list.append(res)
            except: continue
        time.sleep(1)

    if not final_list: return
    res_df = pd.DataFrame(final_list)

    # 4. 深度评级逻辑
    res_df['RS评分'] = res_df['RS_Raw'].rank(pct=True).apply(lambda x: int(x*99))
    sector_scores = res_df.groupby('Sector')['RS评分'].mean().to_dict()
    res_df['Sector_Alpha'] = res_df['Sector'].map(sector_scores).round(1)

    # 综合排序逻辑：RS评级(60%) + 吸筹奖励(20%) + 行业Alpha(20%)
    res_df['Commander_Score'] = (res_df['RS评分'] * 0.6) + (res_df['UD_Ratio'] * 15) + (res_df['Sector_Alpha'] * 0.2)
    
    # 剔除超买 (Ext_50 > 22%)
    res_df = res_df[res_df['Ext_50'] < 22]
    final_output = res_df.sort_values(by="Commander_Score", ascending=False).head(55)

    # 5. 精准推送
    sh = init_v35_sheet()
    sh.clear()
    
    header = [[f"📊 大盘天气: {mkt_weather}", f"📡 信号更新: {now_str}", "💡 战术: 优先关注 '口袋起爆' 且 UD_Ratio > 1.2 的标的", "", "", "", "", ""]]
    sh.update(range_name="A1", values=header)

    cols = ["Ticker", "Name", "Action", "Sector", "RS评分", "Sector_Alpha", "Price", "Ext_50", "Tightness", "UD_Ratio", "ADR", "Stop_Loss"]
    sh.update(range_name="A3", values=[cols] + final_output[cols].values.tolist(), value_input_option="USER_ENTERED")

    # 6. 自动美化
    set_frozen(sh, rows=3)
    format_cell_range(sh, 'A3:L3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0, 0, 0)))
    
    rules = get_conditional_format_rules(sh)
    # 高吸筹比绿色标记
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('J4:J100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['1.19']),
                                format=cellFormat(textFormat=textFormat(bold=True, color=color(0, 0.5, 0)), backgroundColor=color(0.8, 1, 0.8)))))
    # 口袋突破特殊黄色标记
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('C4:C100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🎯']),
                                format=cellFormat(backgroundColor=color(1, 0.9, 0.6)))))
    rules.save()

    print(f"✅ V35.0 任务完成。已锁定 {len(final_output)} 支高质量标的。")

if __name__ == "__main__":
    main()
