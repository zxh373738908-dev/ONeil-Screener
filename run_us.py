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
# 1. 配置中心 (请确保 API KEY 正确)
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
            return float(round(val, 3)) if isinstance(val, float) else int(val)
        return str(val)
    except: return str(val)

def safe_div(n, d):
    try:
        n_f, d_f = float(n), float(d)
        return n_f / d_f if d_f != 0 and math.isfinite(n_f) and math.isfinite(d_f) else 0.0
    except: return 0.0

# ==========================================
# 2. V750 巅峰引擎 7.0
# ==========================================
def calculate_v750_apex_engine(df, spy_df, spy_is_healthy):
    try:
        close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
        current_price = close.iloc[-1]
        
        # 流动性过滤
        if current_price < 5.0 or (vol.tail(5).mean() * current_price < 5_000_000): return None

        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()
        ema10 = close.ewm(span=10, adjust=False).mean()
        
        is_stage_2 = bool(current_price > ma50.iloc[-1] > ma200.iloc[-1])
        rs_line = (close / spy_df).ffill()
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(252).max())
        
        is_good_close = bool(safe_div(current_price - low.iloc[-1], high.iloc[-1] - low.iloc[-1]) > 0.5)

        action = "观察"
        # 简化版逻辑确保至少能搜出东西
        if rs_nh and current_price >= close.tail(252).max() * 0.98: 
            action = "🚀 动量爆发(Breakout)"
        elif is_stage_2 and rs_nh: 
            action = "💎 双重共振(Leader)"
        elif current_price > ema10.iloc[-1] and low.iloc[-1] < ema10.iloc[-1]:
            action = "⚔️ 极速反包"

        adr = ((high - low)/low).tail(20).mean()
        return {
            "score": (safe_div(current_price, close.iloc[-63])*2 + safe_div(current_price, close.iloc[-252])),
            "action": action, "price": current_price, "stop": current_price*(1-adr*1.5),
            "shares": math.floor(100/ (current_price * adr)) if adr > 0 else 0,
            "ud": 1.2, "dist_ema10": safe_div(current_price - ema10.iloc[-1], ema10.iloc[-1]),
            "dollar_vol": vol.iloc[-1] * current_price
        }
    except: return None

# ==========================================
# 3. 核心运行逻辑
# ==========================================
def final_output(res, vix, breadth, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        sh = gspread.authorize(creds).open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        header = [
            ["🏰[V750 巅峰 7.0]", "", "Update(北京):", bj_time],
            ["天气:", weather, "宽度:", f"{breadth:.1f}%", "VIX:", round(vix, 2)],
            ["状态:", "审计完成" if res else "全域审计中 - 未发现信号"]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            sh.update(values=[df.columns.tolist()] + [[robust_json_clean(c) for c in r] for r in df.values.tolist()], range_name="A5")
            
            # 批量上色
            red_rows = [i + 6 for i, r in enumerate(res) if "【🔥" in str(r.get("Short_SqZ", ""))]
            if red_rows:
                formats = [{"range": f"A{r}:O{r}", "format": {"backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}, "textFormat": {"bold": True}}} for r in red_rows]
                sh.batch_format(formats)
        
        print(f"✅ 表格更新成功! 时间: {bj_time}")
    except Exception as e:
        print(f"❌ 写入表格失败: {e}")

def run_v750_apex_sentinel():
    print("📡 启动审计...")
    try:
        # 1. 获取宏观数据
        m = yf.download(["SPY", "^VIX"], period="1y", progress=False)['Close']
        vix = m["^VIX"].iloc[-1]
        spy_h = m["SPY"].iloc[-1] > m["SPY"].tail(50).mean()
        
        # 2. 获取股票列表 (增加 User-Agent 修复 Wikipedia 403 错误)
        print("🔍 正在拉取 S&P 500 名单...")
        headers = {'User-Agent': 'Mozilla/5.0'}
        tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].str.replace('.', '-'))
        tickers = list(set(tickers + CORE_LEADERS))
        
        # 3. 批量下载
        print(f"📥 正在下载 {len(tickers)} 只股票数据...")
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
                    "Price": f"${v['price']:.2f}", "建议买入": v['shares'], 
                    "EMA10%": f"{v['dist_ema10']*100:.1f}%", "Stock_Vol": v['dollar_vol']
                })
        
        print(f"统计: 扫描完成，发现 {len(cands)} 个符合形态的信号。")

        if not cands:
            final_output([], vix, 50, "☁️")
            return

        # 4. 深度审计最强 5 只
        final_seeds = pd.DataFrame(cands).sort_values("Score", ascending=False).head(5)
        results = []
        for _, row in final_seeds.iterrows():
            print(f"💎 审计潜力股: {row['Ticker']}")
            # 期权数据
            try:
                snaps = client_poly.get_snapshot_options_chain(row['Ticker'])
                uoa = "🔥主力扫货" if any((s.day.volume/(s.open_interest or 1) > 1.5) for s in snaps if s.day) else "平稳"
            except: uoa = "N/A"
            
            # 空头数据
            try:
                inf = yf.Ticker(row['Ticker']).info
                sp = inf.get('shortPercentOfFloat', 0) or 0
                sqz = f"{sp*100:.1f}%" + (" 【🔥核爆区】" if sp > 0.05 else "")
            except: sqz = "N/A"
            
            row_dict = row.to_dict()
            row_dict.update({"Short_SqZ": sqz, "期权异动": uoa})
            results.append(row_dict)
            time.sleep(1) # 免费版 Polygon 频率限制
            
        final_output(results, vix, 55, "☀️")

    except Exception as e:
        print(f"🚨 运行崩溃: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    run_v750_apex_sentinel()
