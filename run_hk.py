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
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit#gid=0"
CREDS_FILE = "credentials.json"
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def init_v27_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_url(OUTPUT_SHEET_URL).get_worksheet(0)

# ==========================================
# 🧠 2. V27 核心引擎 (RS Accel + VDU + ADR)
# ==========================================

def calculate_v27_metrics(df_h, hsi_df):
    """V27 深空狙击：RS加速 + 窒息量 + 活跃度分析"""
    if df_h.empty or len(df_h) < 250: return None
    
    close = df_h['Close'].values
    high = df_h['High'].values
    low = df_h['Low'].values
    vol = df_h['Volume'].values
    cp = close[-1]
    
    # A. ADR (平均日波幅) - 过滤死鱼股
    # 规则: 过去20天平均日波幅(High-Low)/Close 必须 > 2.2%
    adr = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
    if adr < 2.2: return None 

    # B. VDU (Volume Dry Up) 窒息量检测
    # 规则: 今日成交量 < 50日平均成交量的 45% (代表卖盘彻底枯竭)
    avg_vol_50 = np.mean(vol[-50:])
    is_vdu = vol[-1] < (avg_vol_50 * 0.45)

    # C. RS 加速器 (二阶斜率)
    hsi_close = hsi_df['Close'].reindex(df_h.index).ffill().values
    rs_line = close / hsi_close
    # 计算RS线10日斜率的变化率
    rs_slope_now = (rs_line[-1] - rs_line[-10]) / rs_line[-10]
    rs_slope_prev = (rs_line[-11] - rs_line[-20]) / rs_line[-20]
    rs_accel = rs_slope_now - rs_slope_prev # 加速度

    # D. 波幅扩张 (Range Expansion)
    # 规则: 今日波幅 > 过去10日平均波幅的 1.6 倍 (起爆信号)
    daily_range = high - low
    avg_range_10 = np.mean(daily_range[-11:-1])
    is_expanding = daily_range[-1] > (avg_range_10 * 1.6)

    # E. 均线系统与超买过滤
    ma50 = np.mean(close[-50:])
    ma200 = np.mean(close[-200:])
    ext50 = (cp / ma50 - 1) * 100
    
    label = "📡 轨道维持"
    if is_expanding and cp > ma50: label = "🚀 深度起爆 (Range Expansion)"
    elif is_vdu and cp > ma50: label = "💎 窒息枯竭 (能量蓄势)"
    elif rs_accel > 0 and rs_slope_now > 0: label = "🔥 引擎加速 (RS Accel)"
    elif cp < ma50 and cp > ma200: label = "⏳ 结构回调"

    return {
        "Price": round(cp, 2),
        "ADR": round(adr, 2),
        "RS_Raw": rs_line[-1] * 100,
        "RS_Accel": round(rs_accel * 100, 4),
        "VDU": "YES" if is_vdu else "",
        "Action": label,
        "Ext50": round(ext50, 1),
        "StopLoss": round(cp - (np.mean(high[-20:]-low[-20:]) * 2.0), 2)
    }

# ==========================================
# 🚀 3. 主作战流程
# ==========================================

def main():
    print(f"[{datetime.datetime.now(TZ_SHANGHAI).strftime('%H:%M')}] 🚀 V27.0 深空狙击系统启动...")
    
    # 1. 指数背景
    hsi_df = yf.download("^HSI", period="350d", progress=False)
    hsi_cp = hsi_df['Close'].iloc[-1]
    is_safe = hsi_cp > hsi_df['Close'].rolling(50).mean().iloc[-1]

    # 2. 获取池子 (百亿流动性)
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "description", "close", "market_cap_basic", "sector"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1.2e10},
                          {"left": "close", "operation": "greater", "right": 1.0}],
               "range": [0, 450], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    tv_data = requests.post(url, json=payload, timeout=15).json().get('data', [])
    df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "Others"} for d in tv_data])

    # 3. 批量演算
    tickers = [str(c).zfill(4)+".HK" for c in df_pool['code']]
    full_data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)
    
    final_list = []
    for _, row in df_pool.iterrows():
        t = str(row['code']).zfill(4)+".HK"
        try:
            m = calculate_v27_metrics(full_data[t].dropna(), hsi_df)
            if m:
                m.update({"代码": row['code'], "行业": row['sector']})
                final_list.append(m)
        except: continue

    if not final_list: return
    res_df = pd.DataFrame(final_list)
    
    # 4. 排名逻辑：RS 百分位排名
    res_df['RS评级'] = res_df['RS_Raw'].rank(pct=True).apply(lambda x: int(x*99))
    
    # 5. 行业共振 (Sector Alpha)
    sector_alpha = res_df.groupby('行业')['RS评级'].mean().to_dict()
    res_df['行业Alpha'] = res_df['行业'].map(sector_alpha).round(1)

    # 6. 综合狙击评分 (RS排名 50% + 加速 20% + 行业 20% + ADR 10%)
    res_df['狙击得分'] = (res_df['RS评级'] * 0.5) + (res_df['行业Alpha'] * 0.2) + (res_df['RS_Accel'] * 100) + (res_df['ADR'] * 2)
    
    # 7. 市场广度
    breadth = (res_df['RS评级'] > 80).mean() * 100
    pos_guide = "🟢 激进战斗" if (is_safe and breadth > 30) else "🟡 轻仓侦察"

    final_output = res_df.sort_values(by="狙击得分", ascending=False).groupby('行业').head(3)
    final_output = final_output[["代码", "RS评级", "Action", "狙击得分", "VDU", "ADR", "Ext50", "行业", "StopLoss"]].head(35)

    # 8. 写入 Sheets
    sh = init_v27_sheet()
    sh.clear()
    sh.update([
        [f"大盘: {'☀️进攻' if is_safe else '❄️防御'}", f"市场广度: {int(breadth)}%", f"策略建议: {pos_guide}", f"Time: {datetime.datetime.now(TZ_SHANGHAI).strftime('%H:%M')}", "", "", "", "", ""]
    ] + [final_output.columns.values.tolist()] + final_output.values.tolist(), "A1")
    
    # 视觉美化
    set_frozen(sh, rows=2)
    format_cell_range(sh, 'C3:C100', cellFormat(textFormat=textFormat(bold=True), horizontalAlignment='CENTER'))
    # VDU 窒息量特殊蓝色标记
    rule1 = ConditionalFormatRule(ranges=[GridRange.from_a1_range('E3:E100', sh)],
                                booleanRule=BooleanRule(condition=BooleanCondition('TEXT_NOT_EMPTY'), 
                                format=cellFormat(backgroundColor=color(0.8, 0.9, 1), textFormat=textFormat(bold=True))))
    set_conditional_format_rules(sh, [rule1])
    
    print(f"✅ V27.0 任务完成！选出 {len(final_output)} 支深空狙击标的。")

if __name__ == "__main__":
    main()
