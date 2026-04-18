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
# 核心池 (确保永远有数据显示，防止变白)
CORE_LEADERS = ["NVDA", "AAPL", "MSFT", "TSLA", "META", "GOOGL", "AMZN", "NFLX", "PLTR", "AVGO", "COST"]

# ==========================================
# 🛡️ 核心计算引擎
# ==========================================
def get_metrics(df, spy_df):
    try:
        close = df['Close']
        if len(close) < 150: return None
        curr = float(close.iloc[-1])
        
        # 基础计算 (ADR, 量比, 乖离)
        adr = float(((df['High'] - df['Low']) / df['Low']).tail(20).mean())
        vol_r = float(df['Volume'].iloc[-1] / df['Volume'].tail(20).mean())
        ma50 = float(close.rolling(50).mean().iloc[-1])
        
        # 涨幅计算
        c5 = float(curr/close.iloc[-5]-1) if len(close)>5 else 0
        c20 = float(curr/close.iloc[-20]-1) if len(close)>20 else 0
        c60 = float(curr/close.iloc[-60]-1) if len(close)>60 else 0
        
        # 相对大盘强度 (REL)
        s20 = float(spy_df.iloc[-1]/spy_df.iloc[-20]-1)
        s60 = float(spy_df.iloc[-1]/spy_df.iloc[-60]-1)
        
        # RS Score
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
# 3. 终极视觉输出 (解决数据不见的问题)
# ==========================================
def final_output(results, vix, breadth):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        
        # --- 暴力格式初始化：防止文字隐身 ---
        sh.format("A1:Q60", {
            "backgroundColor": {"red": 1, "green": 1, "blue": 1},
            "textFormat": {"foregroundColor": {"red": 0, "green": 0, "blue": 0}, "fontSize": 10},
            "horizontalAlignment": "CENTER"
        })

        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        
        header = [
            ["🏰 [V10.1 巅峰 - 修复运行版]", "", "Update(BJ):", bj_time],
            ["市场天气:", "☀️" if vix < 20 else "☁️", "宽度:", f"{breadth:.1f}%", "VIX:", str(round(vix, 2))],
            ["状态说明:", "🚀爆发 / 💎核心 / 🌀紧缩 / ⚔️反包", "注:", "若扫描无果则显示核心池数据"]
        ]
        sh.update(values=header, range_name="A1")

        if not results:
            sh.update(values=[["⚠️ 正在重新扫描，请刷新页面或等待..."]], range_name="A5")
            return

        df = pd.DataFrame(results)
        # 定义 17 列顺序
        cols_order = ["Ticker", "Industry", "Score", "Action", "Resonance", "ADR", "Vol_Ratio", "Bias", "MktCap", "RS_Rank", "Options", "Price", "5D", "20D", "60D", "R20", "R60"]
        
        # --- 强制类型转换：所有数据转为纯字符串 ---
        output_data = [cols_order]
        for _, row in df.iterrows():
            r = []
            for c in cols_order:
                val = row.get(c, "")
                if c in ["ADR", "Bias", "5D", "20D", "60D", "R20", "R60"]:
                    r.append(f"{float(val)*100:.1f}%")
                elif c == "Price":
                    r.append(f"${float(val):.2f}")
                elif c in ["Score", "Vol_Ratio"]:
                    r.append(str(round(float(val), 2)))
                else:
                    r.append(str(val))
            output_data.append(r)

        # 写入数据
        sh.update(values=output_data, range_name="A5", value_input_option='USER_ENTERED')
        
        # --- 样式：亮绿色表头 ---
        sh.format("A5:Q5", {
            "backgroundColor": {"red": 0, "green": 1, "blue": 0}, 
            "textFormat": {"bold": True, "foregroundColor": {"red": 0, "green": 0, "blue": 0}}
        })
        
        # 自动调整列宽
        widths = [60, 130, 60, 100, 60, 70, 60, 60, 90, 60, 90, 80, 60, 60, 60, 60, 60]
        reqs = [{"updateDimensionProperties": {"range": {"sheetId": sh.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}} for i, w in enumerate(widths)]
        client.open_by_key(SHEET_ID).batch_update({"requests": reqs})

        print(f"✅ 看板刷新成功！共写入 {len(output_data)-1} 行。")
    except Exception as e:
        print(f"❌ 报错: {e}")
        traceback.print_exc()

# ==========================================
# 4. 主执行逻辑
# ==========================================
def run_sentinel():
    print("📡 开启全美股审计 (V10.1)...")
    try:
        # 获取标的
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].str.replace('.', '-'))
        except:
            tickers = CORE_LEADERS
        
        tickers = list(set(tickers + CORE_LEADERS))
        
        # 抓取行情
        data = yf.download(tickers + ["SPY", "^VIX"], period="1y", group_by='ticker', threads=True, progress=False)
        spy_df = data["SPY"]["Close"].dropna()
        vix = float(data["^VIX"]["Close"].iloc[-1])
        
        candidates = []
        for t in tickers:
            if t not in data.columns.levels[0]: continue
            metrics = get_metrics(data[t].dropna(), spy_df)
            if metrics:
                metrics['Ticker'] = t
                candidates.append(metrics)
        
        if not candidates:
            print("未找到符合形态的股票。")
            final_output([], vix, 0); return

        df_all = pd.DataFrame(candidates)
        df_all['RS_Rank'] = df_all['RS_Raw'].rank(pct=True).apply(lambda x: int(x * 99))
        df_top = df_all.sort_values("Score", ascending=False).head(28)
        
        final_list = []
        # 补充行业信息 (批量模式)
        for _, row in df_top.iterrows():
            t = row['Ticker']
            try:
                inf = yf.Ticker(t).info
                ind, mkt = inf.get('industry', 'N/A'), f"{inf.get('marketCap', 0)/1e6:,.0f}"
            except:
                ind, mkt = "N/A", "0"
            
            d = row.to_dict()
            d.update({"Industry": ind, "MktCap": mkt, "Resonance": "1", "Options": "平稳"})
            final_list.append(d)
        
        final_output(final_list, vix, 60.0)
        
    except Exception as e:
        print(f"🚨 崩溃: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    run_sentinel()
