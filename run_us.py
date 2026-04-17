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
            return float(round(val, 3)) if isinstance(val, float) else int(val)
        return str(val)
    except: return str(val)

def safe_div(n, d):
    try:
        n_f, d_f = float(n), float(d)
        return n_f / d_f if d_f != 0 and math.isfinite(n_f) and math.isfinite(d_f) else 0.0
    except: return 0.0

# ==========================================
# 2. V750 巅峰引擎 7.0 (逼空核心)
# ==========================================
def calculate_v750_apex_engine(df, spy_df, spy_is_healthy):
    try:
        close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
        current_price = close.iloc[-1]
        dollar_vol = (vol.tail(5) * close.tail(5)).mean()
        
        if current_price < 10.0 or dollar_vol < 10_000_000: return None

        ma20, ma50 = close.rolling(20).mean(), close.rolling(50).mean()
        ma150, ma200 = close.rolling(150).mean(), close.rolling(200).mean()
        ema10 = close.ewm(span=10, adjust=False).mean()
        vol_ma20 = vol.rolling(20).mean()
        
        # VWAP 近似成本线
        tp = (high.tail(3) + low.tail(3) + close.tail(3)) / 3
        v_tail = vol.tail(3)
        vwap_3d = safe_div((tp * v_tail).sum(), v_tail.sum()) or current_price
        
        is_stage_2 = bool(current_price > ma50.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1])
        rs_line = (close / spy_df).ffill()
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(252).max())
        
        ud_ratio = safe_div(vol[close > close.shift(1)].tail(50).sum(), vol[close < close.shift(1)].tail(50).sum())
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean()) * 100
        is_good_close = bool(safe_div(current_price - low.iloc[-1], high.iloc[-1] - low.iloc[-1]) > 0.55)
        dist_ema10 = safe_div(current_price - ema10.iloc[-1], ema10.iloc[-1])

        is_morning_trap = bool(is_good_close and low.iloc[-1] < ema10.iloc[-1] and current_price > ema10.iloc[-1] and current_price > vwap_3d)
        
        action = "观察"
        if is_morning_trap: action = "⚔️ 早盘诱空反包"
        elif rs_nh and current_price >= close.tail(252).max(): action = "🚀 动量爆发(Breakout)"
        elif is_stage_2 and rs_nh: action = "💎 双重共振(Leader)"
        
        adr = ((high - low)/low).tail(20).mean()
        shares = math.floor((ACCOUNT_SIZE * 0.01) / (current_price * adr * 1.8)) if adr > 0 else 0

        return {
            "score": (safe_div(current_price, close.iloc[-63])*2 + safe_div(current_price, close.iloc[-252])),
            "action": action, "price": current_price, "stop": current_price*(1-adr*1.8),
            "shares": shares, "ud": ud_ratio, "tight": tightness, "adr": adr*100, 
            "rs_nh": rs_nh, "dist_ema10": dist_ema10, "dollar_vol": dollar_vol
        }
    except: return None

# ==========================================
# 3. 扫描逻辑
# ==========================================
def get_apex_uoa_intel(ticker):
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        total, calls, max_v_oi = 0, 0, 0
        for s in snaps:
            v = s.day.volume if s.day else 0
            if v > 50:
                max_v_oi = max(max_v_oi, v / (s.open_interest or 1))
                val = v * (s.day.last or 0) * 100
                total += val
                if s.details.contract_type == 'call': calls += val
        return ("🔥主力扫货" if max_v_oi > 1.5 else "平稳"), round(safe_div(calls, total)*100, 1), total
    except: return "N/A", 50.0, 0

def final_output(res, vix, breadth, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        sh = gspread.authorize(creds).open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        sh.format("A1:Z100", {"backgroundColor": {"red": 1, "green": 1, "blue": 1}, "numberFormat": {"type": "AUTOMATIC"}})
        
        bj_time = (datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8)))).strftime('%Y-%m-%d %H:%M')
        header = [["🏰[V750 巅峰 7.0]", "", "Update:", bj_time], ["天气:", weather, "宽度:", f"{breadth:.1f}%", "VIX:", round(vix, 2)]]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            sh.update(values=[df.columns.tolist()] + [[robust_json_clean(c) for c in r] for r in df.values.tolist()], range_name="A5")
            sh.format("A5:O5", {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}, "textFormat": {"bold": True}})
            
            # 🚀 批量涂色逻辑 (修复 SyntaxError)
            red_rows = [i + 6 for i, r in enumerate(res) if "【🔥" in str(r.get("Short_SqZ", ""))]
            if red_rows:
                formats = []
                for r_idx in red_rows:
                    formats.append({
                        "range": f"A{r_idx}:O{r_idx}",
                        "format": {
                            "backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8},
                            "textFormat": {"bold": True, "foregroundColor": {"red": 0.8, "green": 0, "blue": 0}}
                        }
                    })
                sh.batch_format(formats)
        print(f"✅ 执行完毕: {bj_time}")
    except Exception as e: print(f"❌ 写入失败: {e}")

def run_v750_apex_sentinel():
    print("📡 启动...")
    try:
        m = yf.download(["SPY", "^VIX"], period="1y", progress=False)['Close']
        vix, spy_h = m["^VIX"].iloc[-1], (m["SPY"].iloc[-1] > m["SPY"].tail(50).mean())
        tickers = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]['Symbol'].str.replace('.', '-')) + CORE_LEADERS
        data = yf.download(list(set(tickers + ["SPY"])), period="2y", group_by='ticker', threads=True, progress=False)
        spy_df = data["SPY"]["Close"].dropna()
    except: return

    cands = []
    for t in [x for x in tickers if x in data.columns.levels[0]]:
        v = calculate_v750_apex_engine(data[t].dropna(), spy_df, spy_h)
        if v and v['action'] != "观察":
            cands.append({
                "Ticker": t, "Action": v['action'], "Score": round(v['score'], 2), 
                "Price": f"${v['price']:.2f}", "建议买入": f"{v['shares']}股", "止损": f"${v['stop']:.2f}",
                "U/D": f"{v['ud']:.2f}", "EMA10%": f"{v['dist_ema10']*100:.1f}%", "Stock_Vol": v['dollar_vol']
            })

    if not cands: final_output([], vix, 50, "☁️"); return
    final_seeds = pd.DataFrame(cands).sort_values("Score", ascending=False).head(5)
    results = []
    for _, row in final_seeds.iterrows():
        uoa, cp, ov = get_apex_uoa_intel(row['Ticker'])
        try:
            inf = yf.Ticker(row['Ticker']).info
            sp, sr = inf.get('shortPercentOfFloat', 0) or 0, inf.get('shortRatio', 0) or 0
        except: sp, sr = 0, 0
        
        sqz = f"{sp*100:.1f}%({sr}D)" + (" 【🔥核爆区】" if sp > 0.05 and sr > 3 else "")
        row_dict = row.to_dict()
        row_dict.update({"Short_SqZ": sqz, "期权异动": uoa, "看涨%": f"{cp}%", "期现比": f"{round(safe_div(ov, row['Stock_Vol'])*100, 1)}%"})
        results.append(row_dict)
        time.sleep(12.5)
    
    final_output(results, vix, 55, "☀️")

if __name__ == "__main__":
    try: run_v750_apex_sentinel()
    except: traceback.print_exc()
