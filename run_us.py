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
# 3. V11.0 涅槃演算核心
# ==========================================
def calculate_v11_nexus_metrics(df, spy_df):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        
        # 1. 趋势过滤 (Stage 2)
        ma50, ma150, ma200 = close.rolling(50).mean(), close.rolling(150).mean(), close.rolling(200).mean()
        # 严格 Stage 2：收盘 > 150MA > 200MA，且 200MA 向上
        in_stage_2 = bool(close.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1] and ma200.iloc[-1] > ma200.iloc[-20])
        
        # 2. 量价积累确认 (U/D Ratio)
        # 计算过去 50 天：上涨日成交量之和 / 下跌日成交量之和
        up_days_vol = vol[close > close.shift(1)].tail(50).sum()
        dn_days_vol = vol[close < close.shift(1)].tail(50).sum()
        ud_ratio = safe_div(up_days_vol, dn_days_vol)
        
        # 3. 波动率挤压 (Squeeze)
        ma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        upper_bb, lower_bb = ma20 + (2 * std20), ma20 - (2 * std20)
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        atr20 = tr.rolling(20).mean()
        upper_kc, lower_kc = ma20 + (1.5 * atr20), ma20 - (1.5 * atr20)
        is_squeezing = bool(upper_bb.iloc[-1] < upper_kc.iloc[-1] and lower_bb.iloc[-1] > lower_kc.iloc[-1])
        
        # 4. 相对强度与乖离
        rs_line = (close / spy_df).fillna(method='ffill')
        rs_score = safe_div(rs_line.iloc[-1], rs_line.iloc[-252]) # 一年相对强度
        adr_val = float(((high - low) / low).tail(20).mean())
        ext_pct = safe_div(close.iloc[-1] - ma20.iloc[-1], ma20.iloc[-1]) * 100
        
        # 5. 风险收益比
        stop_loss = close.iloc[-1] - (1.5 * atr20.iloc[-1])
        target_price = close.iloc[-1] + (close.iloc[-1] * adr_val * 3)
        rr_ratio = safe_div(target_price - close.iloc[-1], close.iloc[-1] - stop_loss)
        
        # 综合评分
        score = (rs_score * 2.0) + (ud_ratio * 1.5) + (1.0 if is_squeezing else 0)
        
        return {
            "Score": score, "ud_ratio": ud_ratio, "rr": rr_ratio, "is_squeeze": "🔥挤压" if is_squeezing else "释放",
            "in_stage_2": in_stage_2, "ext": ext_pct, "adr": adr_val * 100, "stop": stop_loss, "target": target_price
        }
    except: return None

# ==========================================
# 4. 自动化引擎
# ==========================================
def run_v11_nexus():
    print(f"📡 [1/4] V11.0 涅槃系统启动：正在同步全市场宽度...")
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(ticker_sector_map.keys()) + ["NVDA", "GOOGL", "CF", "PR", "TSLA"]
    except:
        print("⚠️ 爬虫受限，启用备用核心池..."); tickers = ["AAPL","MSFT","NVDA","GOOGL","AMZN","TSLA","META","AVGO","LLY","COST"]
        ticker_sector_map = {t: "Core" for t in tickers}

    # 批量下载 (252天数据以计算200MA)
    data = yf.download(list(set(tickers + ["SPY"])), period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"]["Close"].dropna()
    vix = float(yf.download("^VIX", period="5d", progress=False)['Close'].iloc[-1])

    # 精准宽度计算 (采样 250 只)
    above_50ma, valid_count = 0, 0
    for t in tickers[:250]:
        if t in data.columns.levels[0]:
            c = data[t]["Close"].dropna()
            if len(c) > 50 and c.iloc[-1] > c.tail(50).mean(): above_50ma += 1
            valid_count += 1
    breadth = (above_50ma / valid_count) * 100 if valid_count > 0 else 50

    print(f"🚀 [2/4] 执行行业配额演算 (宽度: {breadth:.1f}%)...")
    candidates = []
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 200: continue
            
            v11 = calculate_v11_nexus_metrics(df, spy_df)
            if not v11: continue
            
            # --- V11 强力过滤器 ---
            if not v11['in_stage_2']: continue      # 必须在上升第二阶段
            if v11['ud_ratio'] < 1.05: continue     # 必须有明显的机构吸筹量
            if v11['ext'] > (5.0 + v11['adr']*0.5): continue # 动态乖离限制
            if v11['rr'] < 1.8: continue            # 盈亏比门槛
            
            candidates.append({
                "Ticker": t, "Sector": ticker_sector_map.get(t, "Other"), "Score": v11['Score'],
                "Price": float(df['Close'].iloc[-1]), "R:R": v11['rr'], "U/D": v11['ud_ratio'],
                "状态": v11['is_squeeze'], "止损位": v11['stop'], "目标位": v11['target'], "ADR": v11['adr']
            })
        except: continue

    # --- 核心改进：行业分散化处理 ---
    if not candidates:
        final_output([], vix, breadth); return

    # 先按分数排，然后每个行业最多取 2 个
    nexus_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    final_seeds = nexus_df.groupby("Sector").head(2).sort_values(by="Score", ascending=False).head(8)

    print(f"🔥 [3/4] 涅槃审计：期权与财报联防...")
    results = []
    for _, row in final_seeds.iterrows():
        try:
            t_obj = yf.Ticker(row['Ticker'])
            cal = t_obj.calendar
            days = (cal.iloc[0, 0].date() - datetime.date.today()).days if (cal is not None and not cal.empty) else 99
        except: days = 99
        
        opt_score, opt_size = get_sentiment(row['Ticker'])
        row_dict = row.to_dict()
        row_dict.update({
            '财报': f"{days}d" if days < 15 else "安全", '期权看涨%': opt_score, '大单规模': opt_size,
            '评级': "💎SSS" if (opt_score > 60 and breadth > 60 and row['状态'] == "🔥挤压") else "🔥强势"
        })
        results.append(row_dict)
        time.sleep(12) # Polygon 限速

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
            [robust_json_clean("🏰 [V11.0 涅槃系统 - 行业均衡版]"), "", robust_json_clean("Update:"), robust_json_clean(datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))],
            [robust_json_clean("市场天气:"), robust_json_clean(weather), robust_json_clean("市场宽度:"), f"{robust_json_clean(breadth)}%"],
            [robust_json_clean("风险控制:"), robust_json_clean("单一行业配额上限: 2 只"), robust_json_clean("VIX指数:"), robust_json_clean(vix)],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "状态", "Price", "止损位", "目标位", "R:R", "U/D", "ADR", "财报", "期权看涨%", "大单规模", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            raw_data = [df.columns.tolist()] + df.values.tolist()
            clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            sh.update(values=clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "📭 今日无符合 Nexus 涅槃准则的信号。")
            
        print(f"🎉 V11.0 涅槃任务圆满完成。天气：{weather}")
    except Exception as e: print(f"❌ 写入崩溃: {e}")

if __name__ == "__main__":
    run_v11_nexus()
