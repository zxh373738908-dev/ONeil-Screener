import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import time
import math
from polygon import RESTClient

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
POLYGON_API_KEY = "您的_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
creds_file = "credentials.json"

# ==========================================
# 2. 核心脱壳工具
# ==========================================
def robust_json_clean(val):
    try:
        if isinstance(val, (pd.Series, np.ndarray)):
            val = val.item() if val.size == 1 else str(val.tolist())
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
# 3. V12 核心算法：老龙回头 + 奇点感知
# ==========================================
def calculate_v12_oracle_metrics(df, spy_df):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        
        # 1. 均线系统 (O'Neil Stage 2)
        ma50, ma150, ma200 = close.rolling(50).mean(), close.rolling(150).mean(), close.rolling(200).mean()
        is_stage_2 = bool(close.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1])
        
        # 2. 相对强度线 (RS Line) - 感知 GOOGL 奇点
        rs_line = (close / spy_df).fillna(method='ffill')
        # RS 先行信号：RS线创21日新高，但价格还没突破
        rs_stealth = bool(rs_line.iloc[-1] >= rs_line.tail(21).max() and close.iloc[-1] < close.tail(21).max())
        
        # 3. 紧致度 (Tightness / VCP)
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean()) * 100
        
        # 4. 成交量枯竭 (Volume Dry-up)
        v_dry = bool(vol.iloc[-1] < vol.tail(10).mean() * 0.85)
        
        # 5. 老龙回头逻辑 (Leader Pullback)
        # 条件：1年内RS极强(这里简化为rs_line斜率)，回踩50MA或150MA附近，且量缩
        dist_to_50ma = safe_div(abs(close.iloc[-1] - ma50.iloc[-1]), ma50.iloc[-1])
        is_pullback = bool(dist_to_50ma < 0.03 and v_dry and is_stage_2)
        
        # 6. U/D Ratio (积累/派发)
        up_v = vol[close > close.shift(1)].tail(50).sum()
        dn_v = vol[close < close.shift(1)].tail(50).sum()
        ud_ratio = safe_div(up_v, dn_v)
        
        # 7. 综合决策
        action = "未知"
        if rs_stealth and tightness < 1.2: action = "👁️暗盘先行(GOOGL)"
        elif is_pullback: action = "🐉老龙回头(回踩)"
        elif close.iloc[-1] >= close.tail(50).max() and ud_ratio > 1.2: action = "🚀垂直爆破"
        
        # 盈亏比计算
        atr = (high - low).rolling(14).mean().iloc[-1]
        stop_loss = close.iloc[-1] - (1.5 * atr)
        target = close.iloc[-1] + (3.0 * atr)
        
        score = (ud_ratio * 2.0) + (1.5 if rs_stealth else 0) + (1.2 if is_pullback else 0)
        
        return {
            "Score": score, "Action": action, "UD": ud_ratio, "Tight": tightness,
            "RS_NH": rs_stealth, "In_Stage2": is_stage_2, "Stop": stop_loss, "Target": target
        }
    except: return None

# ==========================================
# 4. 自动化引擎
# ==========================================
def run_v12_oracle_system():
    print(f"📡 [1/4] V12 天眼系统：正在同步全球领袖名册...")
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(ticker_sector_map.keys()) + ["NVDA", "GOOGL", "CF", "PR", "NTR", "PLTR"]
    except:
        tickers = ["AAPL","MSFT","NVDA","GOOGL","AMZN","TSLA","META","AVGO","CF","PR"]
        ticker_sector_map = {t: "Leaders" for t in tickers}

    data = yf.download(list(set(tickers + ["SPY"])), period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"]["Close"].dropna()
    vix = float(yf.download("^VIX", period="5d", progress=False)['Close'].iloc[-1])

    # 宽度计算 (250只采样)
    above_50ma, valid_count = 0, 0
    for t in tickers[:250]:
        if t in data.columns.levels[0]:
            c = data[t]["Close"].dropna()
            if len(c) > 50 and c.iloc[-1] > c.tail(50).mean(): above_50ma += 1
            valid_count += 1
    breadth = (above_50ma / valid_count) * 100 if valid_count > 0 else 50

    print(f"🚀 [2/4] 执行‘奇点’感知演算 (宽度: {breadth:.1f}%)...")
    candidates = []
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 200: continue
            
            v12 = calculate_v12_oracle_metrics(df, spy_df)
            if not v12 or v12['Action'] == "未知": continue
            if not v12['In_Stage2']: continue
            
            candidates.append({
                "Ticker": t, "Action": v12['Action'], "Score": v12['Score'],
                "Sector": ticker_sector_map.get(t, "Other"), "Price": float(df['Close'].iloc[-1]),
                "U/D比": v12['UD'], "紧致度": v12['Tight'], "止损位": v12['Stop'], "目标位": v12['Target']
            })
        except: continue

    if not candidates:
        final_output([], vix, breadth); return

    # 排序与行业去重 (每个行业最多2个)
    oracle_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    final_seeds = oracle_df.groupby("Sector").head(2).head(8)

    print(f"🔥 [3/4] 哨兵审计：正在接入 Polygon 期权暗盘...")
    results = []
    for _, row in final_seeds.iterrows():
        opt_score, opt_size = get_sentiment(row['Ticker'])
        try:
            t_obj = yf.Ticker(row['Ticker'])
            cal = t_obj.calendar
            eb_str = "⚠️临近" if (cal is not None and not cal.empty and (cal.iloc[0, 0].date() - datetime.date.today()).days <= 7) else "安全"
        except: eb_str = "未知"

        row_dict = row.to_dict()
        row_dict.update({
            '财报': eb_str, '期权看涨%': opt_score, '大单规模': opt_size,
            '评级': "💎SSS+" if (opt_score > 65 and breadth > 60 and "👁️" in row['Action']) else "🔥强势"
        })
        results.append(row_dict)
        time.sleep(12) # 遵守 Polygon 频率限制

    final_output(results, vix, breadth)

def get_sentiment(ticker):
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        bull, total = 0, 0
        for s in snaps:
            vol = s.day.volume if (s.day and s.day.volume) else 0
            if vol > 0:
                val = vol * (s.day.last or 0) * 100
                total += val
                if s.details.contract_type == 'call': bull += val
        return round(safe_div(bull, total)*100, 1) if total > 0 else 50.0, f"${round(total/1e6, 2)}M"
    except: return 50.0, "N/A"

def final_output(res, vix, breadth):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        weather = "☀️ 极佳" if (breadth > 60 and vix < 21) else "⛈️ 严寒" if (breadth < 40 or vix > 28) else "☁️ 阴天"
        
        header = [
            [robust_json_clean("🏰 [V12 天眼系统 - 奇点感知版]"), "", robust_json_clean("Update:"), robust_json_clean(datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))],
            [robust_json_clean("市场天气:"), robust_json_clean(weather), robust_json_clean("市场宽度:"), f"{robust_json_clean(breadth)}%"],
            [robust_json_clean("策略核心:"), robust_json_clean("👁️暗盘先行(GOOGL型) / 🐉老龙回头(强势回踩)"), robust_json_clean("VIX指数:"), robust_json_clean(vix)],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "Price", "止损位", "目标位", "U/D比", "紧致度", "期权看涨%", "大单规模", "财报", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            raw_data = [df.columns.tolist()] + df.values.tolist()
            clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            sh.update(values=clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "📭 今日无奇点信号或老龙回踩标的。")
            
        print(f"🎉 V12 任务圆满完成。天气：{weather}")
    except Exception as e: print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    run_v12_oracle_system()
