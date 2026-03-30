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
# 🛡️ 核心工具：数据净化
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
# 2. V130 奇点核心算法
# ==========================================
def calculate_v130_singularity(df, spy_df):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        
        # 1. 欧奈尔/米勒维尼 趋势模板 (Trend Template)
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()
        is_stage_2 = bool(close.iloc[-1] > ma50.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1])
        
        # 2. RS 奇点先行逻辑
        rs_line = (close / spy_df).fillna(method='ffill')
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(252).max()) # RS线创一年新高
        price_nh = bool(close.iloc[-1] >= close.tail(252).max())
        rs_stealth = rs_nh and not price_nh # RS先行：强度创新高但价格还没涨到位
        
        # 3. VCP 紧致度 (收缩)
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean()) * 100
        
        # 4. 筹码中心 POC
        counts, bin_edges = np.histogram(close.tail(120).values, bins=50, weights=vol.tail(120).values)
        poc = (bin_edges[np.argmax(counts)] + bin_edges[np.argmax(counts)+1]) / 2
        
        # 5. 波动率目标
        adr = ((high - low)/low).tail(20).mean() * 100
        
        # 综合评分
        score = (2.0 if rs_nh else 1.0) * (1.5 if rs_stealth else 1.0) * safe_div(1, tightness)
        
        return {
            "score": score, "is_stage_2": is_stage_2, "rs_stealth": rs_stealth,
            "tight": tightness, "poc": poc, "price": close.iloc[-1],
            "adr": adr, "stock_dollar_vol": (vol.tail(5) * close.tail(5)).mean()
        }
    except: return None

# ==========================================
# 3. 扫描引擎
# ==========================================
def run_v130_singularity():
    print("📡 [1/3] 奇点哨兵：正在探测全市场‘主力扫货’信号...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 获取名册
    try:
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(ticker_sector_map.keys()) + ["NVDA", "GOOGL", "CF", "PR", "PLTR", "TSLA"]
    except:
        tickers = ["NVDA", "GOOGL", "AAPL", "MSFT", "CF", "PR"]

    data = yf.download(list(set(tickers + ["SPY", "^VIX"])), period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"]["Close"].dropna()
    vix = float(data["^VIX"]["Close"].dropna().iloc[-1]) if "^VIX" in data.columns else 20.0

    # 大盘宽度 (200MA)
    above_200ma = 0
    valid_count = 0
    for t in tickers[:150]:
        if t in data.columns.levels[0]:
            c = data[t]["Close"].dropna()
            if len(c) > 200 and c.iloc[-1] > c.tail(200).mean(): above_200ma += 1
            valid_count += 1
    breadth = (above_200ma / valid_count) * 100 if valid_count > 0 else 50

    candidates = []
    print(f"🚀 [2/3] 执行‘奇点’演算 (长期牛股占比: {breadth:.1f}%)...")
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 200: continue
            
            v130 = calculate_v130_singularity(df, spy_df)
            if not v130 or not v130['is_stage_2']: continue
            
            # 过滤：必须具备 RS 强度或 极度紧致
            if v130['score'] > 1.2 or v130['tight'] < 0.8:
                candidates.append({
                    "Ticker": t, "Score": v130['score'], "Sector": ticker_sector_map.get(t, "Other"),
                    "Price": v130['price'], "POC支撑": v130['poc'], "紧致度": v130['tight'],
                    "ADR%": v130['adr'], "Stock_Dollar_Vol": v130['stock_dollar_vol'],
                    "信号": "👁️RS先行" if v130['rs_stealth'] else "🐉VCP挤压"
                })
        except: continue

    if not candidates: final_output([], vix, breadth); return
    cand_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    final_seeds = cand_df.groupby("Sector").head(2).sort_values(by="Score", ascending=False).head(8)

    print(f"🔥 [3/3] 审计‘真·异动’期权流量与财报冲击...")
    results = []
    weather = "☀️ 极佳" if (breadth > 60 and vix < 22) else "⛈️ 风险" if (breadth < 40 or vix > 28) else "☁️ 震荡"

    for _, row in final_seeds.iterrows():
        # 1. 财报审计
        try:
            t_obj = yf.Ticker(row['Ticker'])
            cal = t_obj.calendar
            if cal is not None and not cal.empty:
                e_date = cal.iloc[0, 0].date()
                days = (e_date - datetime.date.today()).days
                e_str = f"{e_date}({days}d)"
                # 财报预期跳空 = ADR * 1.5
                e_move = f"±{round(row['ADR%']*1.5, 1)}%"
            else: e_str = "未知"; e_move = "N/A"
        except: e_str = "未知"; e_move = "N/A"

        # 2. 期权异动 (V/OI 比率审计)
        uoa_status, call_pct, opt_vol = get_uoa_intel(row['Ticker'])
        os_ratio = safe_div(opt_vol, row['Stock_Dollar_Vol'])
        
        row_dict = row.to_dict()
        row_dict.update({
            "财报日": e_str, "预期跳空": e_move, "期权看涨%": call_pct,
            "异动监控": uoa_status, "期现比": f"{round(os_ratio*100, 1)}%",
            "评级": "💎SSS" if (call_pct > 60 and "🔥" in uoa_status and weather == "☀️ 极佳") else "🔥强势"
        })
        results.append(row_dict)
        time.sleep(12) 

    final_output(results, vix, breadth, weather)

def get_uoa_intel(ticker):
    """
    Polygon 深度异动审计：成交量/未平仓量 (V/OI)
    """
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        total_val, call_val = 0, 0
        max_v_oi = 0
        for s in snaps:
            vol = s.day.volume if s.day else 0
            oi = s.open_interest if s.open_interest else 1
            if vol > 100: # 过滤杂碎单
                v_oi = vol / oi
                max_v_oi = max(max_v_oi, v_oi)
                
                val = vol * (s.day.last or 0) * 100
                total_val += val
                if s.details.contract_type == 'call':
                    call_val += val
        
        status = "正常"
        if max_v_oi > 1.5: status = "🔥主力扫货"
        elif max_v_oi > 0.8: status = "⚠️异常放量"
        
        return status, round(safe_div(call_val, total_val)*100, 1), total_val
    except: return "N/A", 50.0, 0.0

def final_output(res, vix, breadth, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        header = [
            ["🏰 [V130 奇点哨兵 - 异动增强版]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["环境天气:", weather, "200MA上方占比:", f"{round(breadth, 1)}%", "VIX:", round(vix, 2)],
            ["异动逻辑:", "【🔥主力扫货】代表单份合约成交量 > 未平仓量，暗示有大户正在紧急建仓。"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "信号", "异动监控", "Price", "POC支撑", "期权看涨%", "期现比", "紧致度", "财报日", "预期跳空", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            raw_data = [df.columns.tolist()] + df.values.tolist()
            clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            sh.update(values=clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "📭 今日暂未发现机构级期权异动信号。")
            
        print(f"🎉 V130 任务圆满完成。天气：{weather}")
    except Exception as e: print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    run_v130_singularity()
