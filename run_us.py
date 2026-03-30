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

# [核心领袖名单] - 确保在爬虫失效时依然有最强标的
MASTER_LIST = ["NVDA", "GOOGL", "CF", "PR", "TSLA", "PLTR", "NTR", "AAPL", "MSFT", "AMZN", "META", "AVGO", "COST", "LLY"]

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
# 2. V350 阿尔法核心演算引擎
# ==========================================
def calculate_v350_metrics(df, spy_df, dxy_down):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        
        # 1. 均线对齐 (Trend Template)
        ma50, ma150, ma200 = close.rolling(50).mean(), close.rolling(150).mean(), close.rolling(200).mean()
        is_stage_2 = bool(close.iloc[-1] > ma50.iloc[-1] > ma200.iloc[-1])
        
        # 2. RS 巅峰评分 (RS Percentile Concept)
        rs_line = (close / spy_df).fillna(method='ffill')
        rs_score = safe_div(rs_line.iloc[-1], rs_line.iloc[-126]) # 半年强度
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(252).max()) # 一年RS新高
        
        # 3. 紧致度 & 奇点判定 (VCP)
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean()) * 100
        rs_stealth = bool(rs_nh and close.iloc[-1] < close.tail(15).max()) # 典型的 GOOGL 起爆前兆
        
        # 4. 老龙回头 & 异动量
        avg_vol = vol.tail(20).mean()
        v_dry = bool(vol.iloc[-1] < avg_vol * 0.65)
        dist_ma50 = safe_div(abs(close.iloc[-1] - ma50.iloc[-1]), ma50.iloc[-1])
        is_pullback = bool(is_stage_2 and dist_ma50 < 0.025 and v_dry)
        
        # 5. 动态 ADR 止损 (更符合个股波性)
        adr = ((high - low)/low).tail(20).mean() # 平均日波幅
        stop = close.iloc[-1] * (1 - adr * 2.0) # 2倍ADR止损
        
        # 6. 综合指令
        if rs_stealth and tightness < 1.1: action = "👁️奇点先行(GOOGL)"
        elif is_pullback: action = "🐉老龙回头(V20)"
        elif is_stage_2 and rs_nh: action = "💎双重共振"
        elif is_stage_2: action = "👑趋势领袖"
        else: action = "观察"
        
        # 宏观加成 (美元走弱加分)
        final_score = rs_score * (1.2 if dxy_down else 1.0) * (2.0 if rs_nh else 1.0)

        return {
            "score": final_score, "action": action, "is_stage_2": is_stage_2,
            "tight": tightness, "price": close.iloc[-1], "stop": stop, 
            "adr": adr * 100, "dollar_vol": (vol.tail(5) * close.tail(5)).mean()
        }
    except: return None

# ==========================================
# 3. 哨兵扫描主程序
# ==========================================
def run_v350_apex():
    print("📡 [1/3] V350 哨兵巅峰：正在分析宏观天气与名册...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # [宏观探测]
    macro = yf.download(["DX-Y.NYB", "^VIX", "SPY"], period="5d", progress=False)['Close']
    dxy_down = bool(macro["DX-Y.NYB"].iloc[-1] < macro["DX-Y.NYB"].iloc[0])
    vix = float(macro["^VIX"].iloc[-1])
    
    # [名册获取 - 双重保障]
    try:
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        sp_list = sp_df['Symbol'].str.replace('.', '-').tolist()
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(set(sp_list + MASTER_LIST))
    except:
        print("⚠️ 爬虫被封，启用 MASTER_LIST 核心防御模式..."); tickers = MASTER_LIST
        ticker_sector_map = {t: "Leaders" for t in tickers}

    # [数据下载 - 针对性优化]
    data = yf.download(list(set(tickers + ["SPY"])), period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"]["Close"].dropna()

    # [市场宽度审计]
    valid_tickers = [t for t in tickers if t in data.columns.levels[0]]
    breadth_count = 0
    for t in valid_tickers[:200]:
        c = data[t]["Close"].dropna()
        if len(c) > 50 and c.iloc[-1] > c.tail(50).mean(): breadth_count += 1
    breadth = (breadth_count / 200) * 100 if len(valid_tickers) > 0 else 50

    print(f"🚀 [2/3] 执行核心演算 (宽度: {breadth:.1f}% / 美元顺风: {dxy_down})...")
    candidates = []
    for t in valid_tickers:
        try:
            df = data[t].dropna()
            if len(df) < 150: continue
            
            v350 = calculate_v350_metrics(df, spy_df, dxy_down)
            if not v350 or v350['action'] == "观察": continue
            
            # 环境熔断逻辑
            if vix > 27 and "🐉" not in v350['action']: continue

            candidates.append({
                "Ticker": t, "Action": v300_action_logic(v350), "Score": v350['score'], 
                "Sector": ticker_sector_map.get(t, "Other"), "Price": v350['price'],
                "止损位": v350['stop'], "ADR%": v350['adr'], "Stock_Dollar_Vol": v350['dollar_vol']
            })
        except: continue

    if not candidates: final_output([], vix, breadth, dxy_down); return
    
    # [行业配额制 + 分数排序]
    cand_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    final_seeds = cand_df.groupby("Sector").head(2).sort_values(by="Score", ascending=False).head(10)

    print(f"🔥 [3/3] 哨兵终审：正在执行【V/OI 异动】与【财报风险】审计...")
    results = []
    weather = "☀️ 极佳" if (breadth > 60 and vix < 21) else "⛈️ 风险" if (breadth < 40 or vix > 28) else "☁️ 震荡"

    for _, row in final_seeds.iterrows():
        # 财报日
        try:
            t_obj = yf.Ticker(row['Ticker'])
            cal = t_obj.calendar
            e_str = cal.iloc[0, 0].date().strftime('%m-%d') if (cal is not None and not cal.empty) else "未知"
        except: e_str = "未知"

        # 期权 V/OI 异动探测
        uoa_status, call_pct, opt_vol = get_apex_option_intel(row['Ticker'])
        os_ratio = safe_div(opt_vol, row['Stock_Dollar_Vol'])
        
        row_dict = row.to_dict()
        row_dict.update({
            "财报日": e_str, "期权看涨%": call_pct, "异动监控": uoa_status, 
            "期现比": f"{round(os_ratio*100, 1)}%",
            "评级": "💎SSS" if (call_pct > 63 and "🔥" in uoa_status and weather != "⛈️ 风险") else "🔥强势"
        })
        results.append(row_dict)
        time.sleep(12.5) 

    final_output(results, vix, breadth, dxy_down, weather)

def v300_action_logic(v):
    # 为特殊个股打标签
    if v['price'] > 50 and v['score'] > 2.0: return v['action']
    return v['action']

def get_apex_option_intel(ticker):
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        total_val, call_val, max_v_oi = 0, 0, 0
        for s in snaps:
            vol = s.day.volume if s.day else 0
            oi = s.open_interest if s.open_interest else 1
            if vol > 50:
                v_oi = vol / oi
                max_v_oi = max(max_v_oi, v_oi)
                val = vol * (s.day.last or 0) * 100
                total_val += val
                if s.details.contract_type == 'call': call_val += val
        
        # 真正的异动审计：成交量/未平仓量比率
        status = "🔥主力扫货" if max_v_oi > 1.4 else "⚠️放量" if max_v_oi > 0.8 else "平稳"
        return status, round(safe_div(call_val, total_val)*100, 1), total_val
    except: return "N/A", 50.0, 0.0

def final_output(res, vix, breadth, dxy_down, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        header = [
            ["🏰 [V350 哨兵巅峰版 - Sentinel Apex]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["环境天气:", weather, "宽度(50MA):", f"{round(breadth, 1)}%", "VIX:", round(vix, 2), "美元顺风:", "✅" if dxy_down else "-"],
            ["作战指令:", "关注【👁️奇点先行】捕获GOOGL/NVDA，【🐉老龙回头】捕获CF/PR回踩。"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "异动监控", "Price", "止损位", "期权看涨%", "期现比", "ADR%", "财报日", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            raw_data = [df.columns.tolist()] + df.values.tolist()
            clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            sh.update(values=clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "📭 今日无泰坦级信号，建议维持现金防御。")
            
        print(f"🎉 V350 任务圆满完成。天气：{weather}")
    except Exception as e: print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    run_v350_apex()
