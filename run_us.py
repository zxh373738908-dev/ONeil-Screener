import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import time
import math
import traceback
from polygon import RESTClient

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
POLYGON_API_KEY = "您的_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
creds_file = "credentials.json"
ACCOUNT_SIZE = 10000  

CORE_LEADERS = ["NVDA", "GOOGL", "CF", "PR", "TSLA", "PLTR", "META", "AVGO", "COST", "AAPL", "MSFT", "AMZN"]

# ==========================================
# 🛡️ 核心工具
# ==========================================
def robust_json_clean(val):
    try:
        if isinstance(val, (pd.Series, np.ndarray)): val = val.item() if val.size == 1 else str(val.tolist())
        if val is None or pd.isna(val): return ""
        if isinstance(val, (float, int, np.floating, np.integer)):
            if not math.isfinite(val): return 0.0
            return float(round(val, 2)) if isinstance(val, float) else int(val)
        return str(val)
    except: return str(val)

def safe_div(n, d):
    try:
        n_f, d_f = float(n), float(d)
        return n_f / d_f if d_f != 0 and math.isfinite(n_f) and math.isfinite(d_f) else 0.0
    except: return 0.0

# ==========================================
# 2. V750 巅峰引擎
# ==========================================
def calculate_v750_apex_engine(df, spy_df, spy_is_healthy):
    try:
        close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
        current_price = close.iloc[-1]
        
        avg_dollar_vol = (vol.tail(5) * close.tail(5)).mean()
        if current_price < 5.0 or avg_dollar_vol < 5_000_000: return None

        ma50 = close.rolling(50).mean()
        ma200 = close.rolling(200).mean()
        ema10 = close.ewm(span=10, adjust=False).mean()
        
        is_stage_2 = bool(current_price > ma50.iloc[-1] * 0.95 and ma50.iloc[-1] > ma200.iloc[-1] * 0.95)
        rs_line = (close / spy_df).ffill()
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(60).max())
        is_good_close = bool(safe_div(current_price - low.iloc[-1], high.iloc[-1] - low.iloc[-1]) > 0.4)

        action = "观察"
        if rs_nh and current_price >= close.tail(126).max() * 0.98: 
            action = "🚀 动量爆发"
        elif is_stage_2 and rs_nh: 
            action = "💎 核心趋势"
        elif current_price > ema10.iloc[-1] and low.iloc[-1] < ema10.iloc[-1] and is_good_close:
            action = "⚔️ 极速反包"

        adr = ((high - low)/low).tail(20).mean()
        stop_price = current_price * (1 - max(adr, 0.04))
        shares = math.floor(100 / (current_price - stop_price)) if (current_price - stop_price) > 0 else 0

        return {
            "score": (safe_div(current_price, close.iloc[-63])*2 + safe_div(current_price, close.iloc[-126])),
            "action": action, "price": current_price, "stop": stop_price,
            "shares": shares, "dist_ema10": safe_div(current_price - ema10.iloc[-1], ema10.iloc[-1]),
            "dollar_vol": avg_dollar_vol
        }
    except: return None

# ==========================================
# 3. 终极视觉输出引擎 (V8.5)
# ==========================================
def final_output(res, vix, breadth, weather, status_msg="审计完成"):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        
        sh.clear()
        
        # 1. 设置基础格式 (居中对齐 + 字体)
        sh.format("A1:J100", {
            "backgroundColor": {"red": 1, "green": 1, "blue": 1},
            "textFormat": {"foregroundColor": {"red": 0.1, "green": 0.1, "blue": 0.1}, "fontSize": 10},
            "horizontalAlignment": "CENTER"
        })
        
        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        
        header = [
            ["🏰 [V750 巅峰 8.5 - 终极看板版]", "", "Update(北京):", bj_time],
            ["当前天气:", weather, "宽度(50MA):", f"{breadth:.1f}%", "VIX指数:", round(vix, 2)],
            ["运行状态:", status_msg, "策略说明:", "🟩 强势领涨 / 🟨 极速反包 / 🟥 逼空预警"]
        ]
        sh.update(values=header, range_name="A1")
        sh.format("A1:A3", {"horizontalAlignment": "LEFT", "textFormat": {"bold": True}})
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "Action", "Score", "Price", "建议买入(股)", "止损位", "EMA10乖离", "成交额(M)", "Short_SqZ", "期权异动"]
            df = df[cols]
            sh.update(values=[df.columns.tolist()] + [[robust_json_clean(c) for c in r] for r in df.values.tolist()], range_name="A5")
            
            # 2. 表头黑色科技感格式
            sh.format("A5:J5", {
                "backgroundColor": {"red": 0.1, "green": 0.1, "blue": 0.1}, 
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}
            })
            
            # 3. 批量涂色
            formats = []
            for i, r in enumerate(res):
                row_idx = i + 6
                action = r.get("Action", "")
                sqz = str(r.get("Short_SqZ", ""))
                
                # 绿色行
                if "🚀" in action or "💎" in action:
                    formats.append({"range": f"A{row_idx}:J{row_idx}", "format": {"backgroundColor": {"red": 0.9, "green": 1.0, "blue": 0.9}}})
                # 黄色行
                elif "⚔️" in action:
                    formats.append({"range": f"A{row_idx}:J{row_idx}", "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.9}}})
                # 红色预警行
                if "核爆区" in sqz:
                    formats.append({
                        "range": f"A{row_idx}:J{row_idx}", 
                        "format": {
                            "backgroundColor": {"red": 1.0, "green": 0.9, "blue": 0.9},
                            "textFormat": {"foregroundColor": {"red": 0.8, "green": 0, "blue": 0}, "bold": True}
                        }
                    })
            if formats: sh.batch_format(formats)

            # 4. 自动调整列宽 (gspread 基础命令)
            # 这里的数字是像素：Ticker 60, Action 120, Score 60, Price 80, 买入 80, 止损 80, 乖离 80, 成交 80, Short 100, 期权 80
            widths = [60, 120, 60, 80, 100, 80, 100, 100, 120, 80]
            requests = []
            for i, width in enumerate(widths):
                requests.append({
                    "updateDimensionProperties": {
                        "range": {"sheetId": sh.id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
                        "properties": {"pixelSize": width},
                        "fields": "pixelSize"
                    }
                })
            client.open_by_key(SHEET_ID).batch_update({"requests": requests})
            
        print(f"✅ 终极看板刷新成功! 时间: {bj_time}")
    except Exception as e:
        print(f"❌ 报告生成失败: {e}")
        traceback.print_exc()

# ==========================================
# 4. 执行逻辑
# ==========================================
def run_v750_apex_sentinel():
    print("📡 启动 V750 巅峰全域审计...")
    try:
        m = yf.download(["SPY", "^VIX"], period="1y", progress=False)['Close']
        vix = m["^VIX"].iloc[-1]
        spy_h = m["SPY"].iloc[-1] > m["SPY"].tail(50).mean()
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        try:
            tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].str.replace('.', '-'))
        except:
            tickers = CORE_LEADERS
        
        tickers = list(set(tickers + CORE_LEADERS))
        data = yf.download(tickers + ["SPY"], period="2y", group_by='ticker', threads=True, progress=False)
        spy_df = data["SPY"]["Close"].dropna()

        cands = []
        for t in tickers:
            if t not in data.columns.levels[0]: continue
            df_t = data[t].dropna()
            if len(df_t) < 100: continue
            
            v = calculate_v750_apex_engine(df_t, spy_df, spy_h)
            if v and v['action'] != "观察":
                cands.append({
                    "Ticker": t, "Action": v['action'], "Score": round(v['score'], 2), 
                    "Price": f"${v['price']:.2f}", "建议买入(股)": v['shares'], 
                    "止损位": f"${v['stop']:.2f}", "EMA10乖离": f"{v['dist_ema10']*100:.1f}%", 
                    "成交额(M)": f"'{v['dollar_vol']/1_000_000:.1f}M" # 强制加单引号防止自动格式化
                })
        
        if not cands:
            final_output([], vix, 50, "☁️", status_msg="审计完成：未发现符合形态个股")
            return

        final_seeds = pd.DataFrame(cands).sort_values("Score", ascending=False).head(15)
        results = []
        for _, row in final_seeds.iterrows():
            try:
                inf = yf.Ticker(row['Ticker']).info
                sp = inf.get('shortPercentOfFloat', 0) or 0
                sqz = f"{sp*100:.1f}%" + (" 【🔥核爆区】" if sp > 0.08 else "")
            except: sqz = "N/A"
            
            row_dict = row.to_dict()
            row_dict.update({"Short_SqZ": sqz, "期权异动": "平稳"})
            results.append(row_dict)
            
        final_output(results, vix, 55, "☀️" if vix < 20 else "☁️")

    except Exception as e:
        print(f"🚨 运行崩溃: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    run_v750_apex_sentinel()
