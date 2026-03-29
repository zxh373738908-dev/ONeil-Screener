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
from scipy import stats
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
    doc = client.open_by_url(OUTPUT_SHEET_URL)
    # 确保打开第一个工作表
    return doc.get_worksheet(0)

# ==========================================
# 🧠 2. V27.1 核心引擎
# ==========================================

def calculate_v27_metrics(df_h, hsi_df):
    if df_h.empty or len(df_h) < 200: return None
    
    close = df_h['Close'].values
    high = df_h['High'].values
    low = df_h['Low'].values
    vol = df_h['Volume'].values
    cp = close[-1]
    
    # A. ADR 活跃度 (降至 1.8 增加信号量)
    adr = np.mean((high[-20:] - low[-20:]) / close[-20:]) * 100
    if adr < 1.8: return None 

    # B. VDU 窒息量
    avg_vol_50 = np.mean(vol[-50:])
    is_vdu = vol[-1] < (avg_vol_50 * 0.5)

    # C. RS 演算
    try:
        hsi_close = hsi_df['Close'].reindex(df_h.index).ffill().values
        rs_line = close / hsi_close
        rs_slope_now = (rs_line[-1] - rs_line[-10]) / rs_line[-10]
        rs_slope_prev = (rs_line[-11] - rs_line[-20]) / rs_line[-20]
        rs_accel = rs_slope_now - rs_slope_prev
    except:
        return None

    # D. 均线系统
    ma50 = np.mean(close[-50:])
    ma200 = np.mean(close[-200:])
    ext50 = (cp / ma50 - 1) * 100
    
    label = "📡 轨道维持"
    if is_vdu and cp > ma50: label = "💎 窒息枯竭"
    elif rs_accel > 0 and rs_slope_now > 0: label = "🔥 引擎加速"
    elif cp < ma50 and cp > ma200: label = "⏳ 结构回调"

    tr = np.maximum(high - low, np.maximum(abs(high - np.roll(close, 1)), abs(low - np.roll(close, 1))))
    atr = pd.Series(tr).rolling(20).mean().iloc[-1]

    return {
        "Price": round(cp, 2),
        "ADR": round(adr, 2),
        "RS_Raw": rs_line[-1] * 100,
        "RS_Accel": round(rs_accel * 100, 4),
        "VDU": "YES" if is_vdu else "",
        "Action": label,
        "Ext50": round(ext50, 1),
        "StopLoss": round(cp - (atr * 2.0), 2)
    }

# ==========================================
# 🚀 3. 执行流程
# ==========================================

def main():
    print(f"[{datetime.datetime.now(TZ_SHANGHAI).strftime('%H:%M')}] 🚀 V27.1 系统启动...")
    
    # 1. 指数背景
    try:
        hsi_df = yf.download("^HSI", period="350d", progress=False)
        hsi_cp = hsi_df['Close'].iloc[-1]
        is_safe = hsi_cp > hsi_df['Close'].rolling(50).mean().iloc[-1]
        print(f" -> 恒指当前價: {hsi_cp:.2f}, 状态: {'☀️进攻' if is_safe else '❄️防御'}")
    except Exception as e:
        print(f" -> ❌ 恒指数据下载失败: {e}")
        return

    # 2. 获取初筛池
    url = "https://scanner.tradingview.com/hongkong/scan"
    payload = {"columns": ["name", "description", "close", "market_cap_basic", "sector"],
               "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1.0e10},
                          {"left": "close", "operation": "greater", "right": 1.0}],
               "range": [0, 400], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}
    try:
        resp = requests.post(url, json=payload, timeout=15)
        tv_data = resp.json().get('data', [])
        print(f" -> TradingView 初筛池标的数量: {len(tv_data)}")
    except Exception as e:
        print(f" -> ❌ TV名册抓取失败: {e}")
        return

    df_pool = pd.DataFrame([{"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "Others"} for d in tv_data])

    # 3. 批量演算 (改为单线程提高 CI 稳定性)
    final_list = []
    tickers = [str(c).zfill(4)+".HK" for c in df_pool['code']]
    print(f" -> 正在演算 K 线数据，请稍候...")
    
    # 分批下载防封
    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=False)
    
    for _, row in df_pool.iterrows():
        t = str(row['code']).zfill(4)+".HK"
        try:
            m = calculate_v27_metrics(data[t].dropna(), hsi_df)
            if m:
                m.update({"代码": row['code'], "行业": row['sector']})
                final_list.append(m)
        except: continue

    print(f" -> 筛选完成，符合条件标的: {len(final_list)} 支")

    # 4. 处理结果并写入
    sh = init_v27_sheet()
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d %H:%M')
    
    if not final_list:
        sh.clear()
        sh.update("A1", [[f"最後更新: {now_str} | 今日無符合條件標的 (ADR/VDU門檻未達)"]])
        print("⚠️ 今日无信号，已更新时间戳。")
        return

    res_df = pd.DataFrame(final_list)
    res_df['RS评级'] = res_df['RS_Raw'].rank(pct=True).apply(lambda x: int(x*99))
    sector_alpha = res_df.groupby('行业')['RS评级'].mean().to_dict()
    res_df['行业Alpha'] = res_df['行业'].map(sector_alpha).round(1)
    res_df['狙击得分'] = (res_df['RS评级'] * 0.5) + (res_df['行业Alpha'] * 0.2) + (res_df['RS_Accel'] * 100) + (res_df['ADR'] * 2)
    
    final_output = res_df.sort_values(by="狙击得分", ascending=False).groupby('行业').head(3)
    final_output = final_output[["代码", "RS评级", "Action", "狙击得分", "VDU", "ADR", "Ext50", "行业", "StopLoss"]].head(35)

    # 5. 更新表格
    sh.clear()
    header = [f"大盘: {'☀️进攻' if is_safe else '❄️防御'}", f"更新: {now_str}", f"选股总数: {len(final_output)}", "", "", "", "", "", ""]
    sh.update([header] + [final_output.columns.values.tolist()] + final_output.values.tolist(), "A1")
    
    # 格式化
    set_frozen(sh, rows=2)
    format_cell_range(sh, 'A1:I1', cellFormat(textFormat=textFormat(bold=True, italic=True), backgroundColor=color(0.9, 0.9, 0.9)))
    format_cell_range(sh, 'A2:I2', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0, 0, 0.4)))
    
    print(f"✅ V27.1 运行成功，数据已推送到 Google Sheets。")

if __name__ == "__main__":
    main()
