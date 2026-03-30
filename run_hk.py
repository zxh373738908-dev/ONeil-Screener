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

def init_v41_sheet():
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
# 🧠 3. V41.0 演算核心：欧米伽指挥官
# ==========================================
def analyze_v41(ticker, name, df_h, mkt_cap, sector, hsi_series):
    if df_h.empty or len(df_h) < 252: return None
    try:
        close = df_h['Close'].values.flatten()
        high = df_h['High'].values.flatten()
        low = df_h['Low'].values.flatten()
        vol = df_h['Volume'].values.flatten()
        cp = float(close[-1])
    except: return None

    # --- A. 基础因子矩阵 ---
    ma20 = np.mean(close[-20:])
    ma50 = np.mean(close[-50:])
    ma200 = np.mean(close[-200:])
    avg_vol50 = np.mean(vol[-50:])
    vol_ratio = vol[-1] / avg_vol50
    
    # 相对强度 (RS Line)
    hsi_aligned = hsi_series.reindex(df_h.index).ffill().values
    rs_line = close / hsi_aligned
    rs_nh_120 = rs_line[-1] >= np.max(rs_line[-120:])
    
    # 乖离率判定 (蓝筹特权)
    ext_50 = (cp / ma50 - 1) * 100
    is_mega = mkt_cap > 2000e8
    if ext_50 > (30 if is_mega else 22): return None

    # --- B. 核心战法引擎 ---
    action = ""
    rank_score = 0
    
    # 1. 【龙抬头】(00700型 - 蓝筹动能)
    # 逻辑：RS线创半年新高 + 站稳MA20 + 机构温和吸筹
    if is_mega and rs_nh_120 and cp > ma20:
        action = "🛰️ 巨頭領航"
        rank_score = 90

    # 2. 【老龙回踩】(02338型 - 回调买点)
    # 逻辑：在MA200上 + 回踩MA21/MA50带宽 + 窒息量(VDU < 0.7倍)
    near_support = any([abs(cp - m)/m < 0.02 for m in [ma20, ma50]])
    vdu = vol[-1] < avg_vol50 * 0.7
    if cp > ma200 and near_support and vdu and not action:
        action = "🐉 老龍低吸"
        rank_score = 85

    # 3. 【凤凰涅槃】(01585型 - 底部反转)
    # 逻辑：在MA200下 + 站上MA20 + 近3日量能持续放大 + RSI底背离倾向
    vol_growth = vol[-1] > vol[-2] and vol[-2] > np.mean(vol[-10:])
    if cp < ma200 and cp > ma20 and vol_growth and not action:
        action = "🐣 鳳凰重生"
        rank_score = 70

    # 兜底：强力多头
    if not action and cp > ma50 and cp > ma200 and rs_line[-1] > np.mean(rs_line[-20:]):
        action = "📈 趨勢持有"
        rank_score = 60
    
    if not action: return None

    # ATR 止损 (蓝筹更紧)
    tr = np.maximum(high - low, np.maximum(abs(high - np.roll(close, 1)), abs(low - np.roll(close, 1))))
    atr = pd.Series(tr).rolling(20).mean().iloc[-1]
    stop_loss = cp - (atr * (1.8 if is_mega else 2.2))

    return {
        "Ticker": ticker.replace(".HK", ""), "Name": name, "Action": action,
        "Price": round(cp, 2), "RS_Rank": 0, # 后续算
        "Ext_50": round(ext_50, 1), "Vol_Ratio": round(vol_ratio, 2),
        "Mkt_Cap": round(mkt_cap/1e8, 1), "Stop_Loss": round(stop_loss, 2),
        "Sector": sector, "rs_raw": rs_line[-1], "base_score": rank_score,
        "is_bull": cp > ma200
    }

# ==========================================
# 🚀 4. 执行流程
# ==========================================
def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    print(f"[{now_str}] 🚀 V41.0 欧米伽指挥官系统启动...")
    
    # 获取指数
    hsi_raw = yf.download("^HSI", period="350d", progress=False)
    hsi_series = hsi_raw['Close'].iloc[:, 0] if isinstance(hsi_raw['Close'], pd.DataFrame) else hsi_raw['Close']
    hsi_p, hsi_ma50 = hsi_series.iloc[-1], hsi_series.rolling(50).mean().iloc[-1]
    mkt_weather = "☀️ 进攻 (Risk-On)" if hsi_p > hsi_ma50 else "❄️ 防御 (Risk-Off)"

    # 获取池子
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {
        "columns": ["name", "description", "close", "market_cap_basic", "sector"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1e10}],
        "range": [0, 450], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
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
                row = df_pool[df_pool['code'] == code_clean].iloc[0]
                res = analyze_v41(t, name_map.get(code_clean, t), data[t].dropna(), row['mkt'], row['sector'], hsi_series)
                if res: final_list.append(res)
            except: continue
        time.sleep(1)

    if not final_list: return
    res_df = pd.DataFrame(final_list)

    # 1. 深度评级 (IBD式百分位)
    res_df['RS_Rank'] = res_df['rs_raw'].rank(pct=True).apply(lambda x: int(x*99))
    
    # 2. 行业去重 (每行业只留前 3 强)
    # 综合分 = 战法基础分 + RS评级*0.4
    res_df['Final_Score'] = res_df['base_score'] + (res_df['RS_Rank'] * 0.4)
    res_df = res_df.sort_values(by="Final_Score", ascending=False)
    res_df = res_df.groupby('Sector').head(3)
    
    final_output = res_df.head(60)

    # 3. 写入表格
    sh = init_v41_sheet()
    sh.clear()
    # 状态行
    bull_pct = int((res_df['is_bull'].sum() / len(df_pool)) * 100)
    sh.update(range_name="A1", values=[[f"🏗️ 狀態: {mkt_weather}", f"📈 廣度: {bull_pct}%", f"🕒 刷新: {now_str}", "💡 戰法: 🛰️-權重領航 | 🐉-強勢回踩 | 🐣-底部反轉", "", "", "", "", ""]])
    
    cols = ["Ticker", "Name", "Action", "RS_Rank", "Price", "Ext_50", "Vol_Ratio", "Sector", "Mkt_Cap", "Stop_Loss"]
    sh.update(range_name="A3", values=[cols] + final_output[cols].values.tolist(), value_input_option="USER_ENTERED")

    # 4. 美化格式
    set_frozen(sh, rows=3)
    format_cell_range(sh, 'A3:J3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0, 0, 0)))
    
    rules = get_conditional_format_rules(sh)
    # 卫星苏醒
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('C4:C100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🛰️']),
                                format=cellFormat(backgroundColor=color(1, 0.9, 0.8), textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0.4, 0))))))
    # 凤凰重生
    rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('C4:C100', sh)],
        booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🐣']),
                                format=cellFormat(backgroundColor=color(1, 1, 0.9), textFormat=textFormat(bold=True, foregroundColor=color(0.5, 0.5, 0))))))
    rules.save()
    print(f"✅ V41.0 任务圆满完成！今日广度: {bull_pct}%")

if __name__ == "__main__":
    main()
