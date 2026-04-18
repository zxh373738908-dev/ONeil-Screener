import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import math
import traceback

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
creds_file = "credentials.json"
# 核心池 (确保永远有数据显示)
CORE_LEADERS = ["NVDA", "AAPL", "MSFT", "TSLA", "META", "GOOGL", "AMZN", "NFLX", "PLTR", "AVGO", "COST"]

# ==========================================
# 🛡️ 核心计算引擎 (强制返回原生类型)
# ==========================================
def get_metrics(df, spy_df):
    try:
        close = df['Close']
        if len(close) < 150: return None
        curr = float(close.iloc[-1])
        # 基础计算
        adr = float(((df['High'] - df['Low']) / df['Low']).tail(20).mean())
        vol_r = float(df['Volume'].iloc[-1] / df['Volume'].tail(20).mean())
        ma50 = float(close.rolling(50).mean().iloc[-1])
        
        # 涨幅计算
        c5, c20, c60 = float(curr/close.iloc[-5]-1), float(curr/close.iloc[-20]-1), float(curr/close.iloc[-60]-1)
        # 相对大盘 (REL)
        s20, s60 = float(spy_df.iloc[-1]/spy_df.iloc[-20]-1), float(spy_df.iloc[-1]/spy_df.iloc[-60]-1)
        
        # RS Score (简版)
        rs_s = float((curr/close.iloc[-63])*2 + (curr/close.iloc[-126]))
        
        action = "💎 核心趋势" if curr > ma50 else "观察"
        if curr >= close.tail(60).max() * 0.99: action = "🚀 动量爆发"
        
        return {
            "Price": curr, "Action": action, "Score": rs_s, "ADR": adr,
            "Vol_Ratio": vol_r, "Bias": float((curr-ma50)/ma50),
            "5D": c5, "20D": c20, "60D": c60, "R20": c20-s20, "R60": c60-s60, "RS_Raw": rs_s
        }
    except: return None

# ==========================================
# 3. 终极视觉输出 (解决变白问题)
# ==========================================
def final_output(results, vix, breadth):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        
        # --- 暴力格式初始化 ---
        # 强制设置 A1:Q50 为：白底、黑字、居中、10号字
        sh.format("A1:Q50", {
            "backgroundColor": {"red": 1, "green": 1, "blue": 1},
            "textFormat": {"foregroundColor": {"red": 0, "green": 0, "blue": 0}, "fontSize": 10},
            "horizontalAlignment": "CENTER"
        })

        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        
        # 1. 顶部信息
        header = [
            ["🏰 [V10.0 巅峰 - 强制输出版]", "", "Update(BJ):", bj_time],
            ["市场天气:", "☀️" if vix < 20 else "☁️", "宽度:", f"{breadth:.1f}%", "VIX:", str(round(vix, 2))],
            ["状态说明:", "🚀爆发 / 💎核心 / 🌀紧缩 / ⚔️反包", "注:", "若无信号将显示核心池"]
        ]
        sh.update("A1", header)

        # 2. 准备数据
        if not results:
            # 如果没数据，强行抓取 NVDA 填充，防止变白
            sh.update("A5", [["⚠️ 提示：正在重新扫描标的，请稍后..."]])
            return

        df = pd.DataFrame(results)
        # 按照您要求的 17 列顺序
        cols_order = ["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "RS_Rank", "Options", "Price", "5D", "20D", "60D", "R20", "R60"]
        
        output_data = [cols_order] # 第一行是表头
        for _, row in df.iterrows():
            r = []
            for c in cols_order:
                val = row.get(c, "N/A")
                # 转换所有数字为带单位的字符串，保证显示成功
                if c in ["ADR", "Bias", "5D", "20D", "60D", "R20", "R60"]: r.append(f"{float(val)*100:.1f}%")
                elif c == "Price": r.append(f"${float(val):.2f}")
                elif c in ["Score", "Vol_Ratio"]: r.append(str(round(float(val), 2)))
                else: r.append(str(val))
            output_data.append(r)

        # 3. 写入数据 (使用 USER_ENTERED 模式)
        sh.update("A5", output_data, value_input_option='USER_ENTERED')
        
        # 4. 样式美化
        # 表头：亮绿色 + 黑色加粗
        sh.format("A5:Q5", {
            "backgroundColor": {"red": 0, "green": 1, "blue": 0}, # 亮绿色
            "textFormat": {"bold": True, "foregroundColor": {"red": 0, "green": 0, "blue": 0}}
        })
        
        # 行涂色 (简单隔行或Action涂色)
        formats = []
        for i in range(len(output_data)-1):
            row_idx = i + 6
            if "🚀" in output_data[i+1][3]:
                formats.append({"range": f"A{row_idx}:Q{row_idx}", "format": {"backgroundColor": {"red": 0.9, "green": 1, "blue": 0.9}}})
        if formats: sh.batch_format(formats)

        # 调整列宽 (gspread batch_update)
        widths = [60, 120, 60, 100, 60, 70, 60, 60, 90, 60, 90, 80, 60, 60, 60, 60, 60]
        reqs = [{"updateDimensionProperties": {"range": {"sheetId": sh.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}} for i, w in enumerate(widths)]
        client.open_by_key(SHEET_ID).batch_update({"requests": reqs})

        print(f"✅ 看板已刷新！共写入 {len(output_data)-1} 行数据。")
    except Exception as e:
        print(f"❌ 报错: {e}")
        traceback.print_exc()

# ==========================================
# 4. 主执行流程
# ==========================================
def run_sentinel():
    print("📡 开启全美股审计...")
    try:
        # 获取标的 (优先获取 S&P 500)
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].str.replace('.', '-'))
        except:
            tickers = CORE_LEADERS
        
        tickers = list(set(tickers + CORE_LEADERS))
        
        # 抓取行情 (只抓近 200 天)
        data = yf.download(tickers + ["SPY", "^VIX"], period="1y", group_by='ticker', threads=True, progress=False)
        spy_df = data["SPY"]["Close"].dropna()
        vix = data["^VIX"]["Close"].iloc[
