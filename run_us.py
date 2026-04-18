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
CORE_LEADERS = ["NVDA", "GOOGL", "CF", "PR", "TSLA", "PLTR", "META", "AVGO", "COST", "AAPL", "MSFT", "AMZN"]

# ==========================================
# 🛡️ 核心计算引擎
# ==========================================
def calculate_metrics(df, spy_df):
    try:
        close = df['Close']
        if len(close) < 150: return None
        curr = float(close.iloc[-1])
        vol_ratio = float(df['Volume'].iloc[-1] / df['Volume'].tail(20).mean())
        rs_raw = float((curr/close.iloc[-63])*2 + (curr/close.iloc[-126]) + (curr/close.iloc[-189]))
        
        # 简单形态判断
        action = "观察"
        if curr >= close.tail(126).max() * 0.98: action = "🚀 动量爆发"
        elif curr > close.rolling(50).mean().iloc[-1]: action = "💎 核心趋势"
        
        return {
            "Price": curr, "Action": action, "Score": rs_raw, 
            "Vol_Ratio": vol_ratio, "RS_Raw": rs_raw,
            "Chg_5D": float(curr / close.iloc[-5] - 1),
            "Chg_20D": float(curr / close.iloc[-20] - 1)
        }
    except: return None

# ==========================================
# 3. 强力输出引擎 (强制文字显示版)
# ==========================================
def final_output(results, vix, breadth, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        
        # --- 步骤 1: 暴力重置所有格式 ---
        sh.clear()
        # 强制全表：白底、黑字、无加粗
        sh.format("A1:P100", {
            "backgroundColor": {"red": 1, "green": 1, "blue": 1},
            "textFormat": {"foregroundColor": {"red": 0, "green": 0, "blue": 0}, "bold": False, "fontSize": 10},
            "horizontalAlignment": "CENTER"
        })
        
        # --- 步骤 2: 写入顶部信息 ---
        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        header_info = [
            ["🏰 [V9.8 巅峰 - 强制显示版]", "", "Update:", bj_time],
            ["市场天气:", weather, "宽度:", f"{breadth:.1f}%", "VIX:", str(round(vix, 2))]
        ]
        sh.update("A1", header_info)
        
        if not results:
            sh.update("A5", [["⚠️ 今日未扫到符合条件的股票"]])
            return

        # --- 步骤 3: 构造纯字符串数据表 ---
        df = pd.DataFrame(results)
        cols = ["Ticker", "Industry", "Score", "Action", "Resonance", "Vol_Ratio", "MktCap(M)", "RS_Rank", "Options", "Price", "5D%", "20D%"]
        df = df[cols]
        
        data_rows = [df.columns.tolist()] # 表头
        for _, row in df.iterrows():
            curr_row = []
            for col in cols:
                val = row[col]
                # 强制将所有类型转为字符串，防止 API 渲染失败
                if col in ["5D%", "20D%"]: curr_row.append(f"{float(val)*100:.1f}%")
                elif col == "Price": curr_row.append(f"${float(val):.2f}")
                elif col in ["Score", "Vol_Ratio"]: curr_row.append(str(round(float(val), 2)))
                else: curr_row.append(str(val))
            data_rows.append(curr_row)

        # 写入主体数据 (USER_ENTERED 模式最稳)
        sh.update("A5", data_rows, value_input_option='USER_ENTERED')
        
        # --- 步骤 4: 重新涂色 (在数据写入后再操作) ---
        # 1. 亮绿色表头 (A5:L5)
        sh.format("A5:L5", {
            "backgroundColor": {"red": 0.0, "green": 0.9, "blue": 0.0}, # 亮绿
            "textFormat": {"bold": True, "foregroundColor": {"red": 0, "green": 0, "blue": 0}} # 强制黑字
        })
        
        # 2. 自动调整列宽
        widths = [60, 130, 60, 100, 75, 65, 85, 65, 90, 75, 65, 65]
        reqs = [{"updateDimensionProperties": {"range": {"sheetId": sh.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}} for i, w in enumerate(widths)]
        client.open_by_key(SHEET_ID).batch_update({"requests": reqs})

        print(f"✅ 刷新成功！共写入 {len(data_rows)-1} 行数据。")
    except Exception as e:
        print(f"❌ 报错: {e}")
        traceback.print_exc()

# ==========================================
# 4. 执行逻辑
# ==========================================
def run_v750_apex_sentinel():
    print("📡 正在扫描市场...")
    try:
        tickers = CORE_LEADERS # 先用核心池测试，确保能显示
        # 如果想全量扫描，取消下面两行的注释
        # headers = {'User-Agent': 'Mozilla/5.0'}
        # tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].str.replace('.', '-'))
        
        data = yf.download(tickers + ["SPY", "^VIX"], period="1y", group_by='ticker', progress=False)
        spy_df = data["SPY"]["Close"].dropna()
        vix = data["^VIX"]["Close"].iloc[-1]
        
        raw_results = []
        for t in tickers:
            if t not in data.columns.levels[0]: continue
            res = calculate_metrics(data[t].dropna(), spy_df)
            if res:
                res['Ticker'] = t
                raw_results.append(res)
        
        if not raw_results: 
            final_output([], vix, 50, "☁️"); return

        df_all = pd.DataFrame(raw_results)
        df_all['RS_Rank'] = df_all['RS_Raw'].rank(pct=True).apply(lambda x: int(x * 99))
        df_top = df_all.sort_values("Score", ascending=False).head(20)
        
        final_list = []
        for _, row in df_top.iterrows():
            try:
                inf = yf.Ticker(row['Ticker']).info
                ind, mkt = inf.get('industry', 'N/A'), inf.get('marketCap', 0)/1_000_000
            except: ind, mkt = "N/A", 0
            
            d = row.to_dict()
            d.update({"Industry": ind, "MktCap(M)": f"{mkt:,.0f}", "Resonance": "1", "Options": "平稳"})
            final_list.append(d)
            
        final_output(final_list, vix, 60, "☀️")
    except Exception as e:
        print(f"🚨 崩溃: {e}"); traceback.print_exc()

if __name__ == "__main__":
    run_v750_apex_sentinel()
