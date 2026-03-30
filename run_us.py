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
# 🛡️ 核心工具：数据净化 (100% 兼容 Google Sheets)
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
# 2. V200 核心演算引擎 (Dual-Engine)
# ==========================================
def calculate_v200_omni_metrics(df, spy_df):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        
        # --- [A] 趋势状态模板 (V100 核心) ---
        ma50, ma150, ma200 = close.rolling(50).mean(), close.rolling(150).mean(), close.rolling(200).mean()
        is_stage_2 = bool(close.iloc[-1] > ma50.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1])
        
        # --- [B] 奇点感知逻辑 (V130 核心) ---
        rs_line = (close / spy_df).fillna(method='ffill')
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(252).max())
        price_nh = bool(close.iloc[-1] >= close.tail(252).max())
        rs_stealth = rs_nh and not price_nh # RS领先于价格
        
        # --- [C] 紧致度与筹码 (VCP/POC) ---
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean()) * 100
        counts, bin_edges = np.histogram(close.tail(120).values, bins=50, weights=vol.tail(120).values)
        poc = (bin_edges[np.argmax(counts)] + bin_edges[np.argmax(counts)+1]) / 2
        
        # --- [D] 评分系统 ---
        v100_score = safe_div(close.iloc[-1], ma200.iloc[-1]) * 10 # 趋势分
        v130_score = (1.5 if rs_stealth else 1.0) * safe_div(2.0, tightness) # 爆发分
        
        # 综合动作标记
        if rs_stealth and tightness < 1.0: action = "👁️奇点先行(V130)"
        elif is_stage_2 and rs_nh: action = "💎双重共振(V+)"
        elif is_stage_2: action = "🐉稳健趋势(V100)"
        else: action = "观察"

        adr = ((high - low)/low).tail(20).mean() * 100
        stop = close.iloc[-1] - (2.5 * (high - low).rolling(14).mean().iloc[-1])
        
        return {
            "score": v100_score + v130_score, "action": action, "is_stage_2": is_stage_2,
            "rs_nh": rs_nh, "tight": tightness, "poc": poc, "price": close.iloc[-1],
            "stop": stop, "adr": adr, "dollar_vol": (vol.tail(5) * close.tail(5)).mean()
        }
    except: return None

# ==========================================
# 3. 全天候扫描器
# ==========================================
def run_v200_omni():
    print("📡 [1/3] V200 全天候指挥部：正在执行市场宽度审计...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(ticker_sector_map.keys()) + ["NVDA", "GOOGL", "CF", "PR", "PLTR", "TSLA"]
    except:
        tickers = ["NVDA", "GOOGL", "AAPL", "MSFT", "CF", "PR"]

    data = yf.download(list(set(tickers + ["SPY", "^VIX"])), period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"]["Close"].dropna()
    vix = float(data["^VIX"]["Close"].dropna().iloc[-1]) if "^VIX" in data.columns else 20.0

    # 市场宽度 (50MA)
    above_50ma = 0
    valid_count = 0
    for t in tickers[:200]:
        if t in data.columns.levels[0]:
            c = data[t]["Close"].dropna()
            if len(c) > 50 and c.iloc[-1] > c.tail(50).mean(): above_50ma += 1
            valid_count += 1
    breadth = (above_50ma / valid_count) * 100 if valid_count > 0 else 50

    print(f"🚀 [2/3] 双引擎演算中 (宽度: {breadth:.1f}% / VIX: {vix:.2f})...")
    candidates = []
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 200: continue
            
            v200 = calculate_v200_omni_metrics(df, spy_df)
            if not v200 or v200['action'] == "观察": continue
            
            # 防御逻辑：VIX高时强制过滤 Stage 2
            if vix > 25 and not v200['is_stage_2']: continue

            candidates.append({
                "Ticker": t, "Action": v200['action'], "Score": v200['score'], 
                "Sector": ticker_sector_map.get(t, "Other"), "Price": v200['price'],
                "止损位": v200['stop'], "POC支撑": v200['poc'], "紧致度": v200['tight'],
                "ADR%": v200['adr'], "Stock_Dollar_Vol": v200['dollar_vol']
            })
        except: continue

    if not candidates: final_output([], vix, breadth); return
    
    # 排序并执行行业配额 (每行业最多2只)
    cand_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    final_seeds = cand_df.groupby("Sector").head(2).sort_values(by="Score", ascending=False).head(8)

    print(f"🔥 [3/3] 正在对候选股进行‘真·异动’及财报审计...")
    results = []
    weather = "☀️ 进攻" if (breadth > 60 and vix < 21) else "⛈️ 防御" if (breadth < 40 or vix > 28) else "☁️ 震荡"

    for _, row in final_seeds.iterrows():
        # 财报日
        try:
            t_obj = yf.Ticker(row['Ticker'])
            cal = t_obj.calendar
            e_str = cal.iloc[0, 0].date() if (cal is not None and not cal.empty) else "未知"
        except: e_str = "未知"

        # 期权异动 (V/OI 逻辑)
        uoa_status, call_pct, opt_vol = get_v200_option_intel(row['Ticker'])
        os_ratio = safe_div(opt_vol, row['Stock_Dollar_Vol'])
        
        row_dict = row.to_dict()
        row_dict.update({
            "财报日": e_str, "期权看涨%": call_pct, "异动监控": uoa_status, 
            "期现比": f"{round(os_ratio*100, 1)}%",
            "评级": "💎SSS" if (call_pct > 60 and "🔥" in uoa_status and weather != "⛈️ 防御") else "🔥强势"
        })
        results.append(row_dict)
        time.sleep(12) 

    final_output(results, vix, breadth, weather)

def get_v200_option_intel(ticker):
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
        
        status = "🔥主力扫货" if max_v_oi > 1.2 else "⚠️异常放量" if max_v_oi > 0.7 else "平稳"
        return status, round(safe_div(call_val, total_val)*100, 1), total_val
    except: return "N/A", 50.0, 0.0

def final_output(res, vix, breadth, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        header = [
            ["🏰 [V200 全天候天基指挥部]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["环境天气:", weather, "200MA上方占比:", f"{round(breadth, 1)}%", "VIX指数:", round(vix, 2)],
            ["智能指令:", "【V100】适合稳健持有，【V130】适合捕捉起爆。若天气为防守，严禁追高。"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "异动监控", "Price", "止损位", "POC支撑", "期权看涨%", "期现比", "紧致度", "财报日", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            raw_data = [df.columns.tolist()] + df.values.tolist()
            clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            sh.update(values=clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "📭 当前市场环境下，未发现符合 V100/V130 标准的阿尔法信号。")
            
        print(f"🎉 V200 任务完成！大盘状态：{weather}")
    except Exception as e: print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    run_v200_omni()
