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

# [核心领袖池 - 确保在任何情况下都被审计]
LEADER_POOL = ["NVDA", "GOOGL", "CF", "PR", "TSLA", "PLTR", "META", "AVGO", "COST", "AAPL", "MSFT", "AMZN", "NFLX", "NTR"]

# ==========================================
# 🛡️ 核心工具：终极数据脱壳
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
# 2. V500 混沌引擎 (多策略并行演算)
# ==========================================
def calculate_v500_engine(df, spy_df, dxy_down):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        
        # --- [1] 趋势审计 (V100/Stage 2) ---
        ma50, ma150, ma200 = close.rolling(50).mean(), close.rolling(150).mean(), close.rolling(200).mean()
        is_stage_2 = bool(close.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1] and ma200.iloc[-1] > ma200.iloc[-10])
        
        # --- [2] RS 奇点感知 (V130/V380) ---
        rs_line = (close / spy_df).fillna(method='ffill')
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(252).max()) # 一年相对强度新高
        rs_stealth = bool(rs_nh and close.iloc[-1] < close.tail(20).max() * 1.02) # RS先行
        rs_accel = safe_div(rs_line.iloc[-1] - rs_line.iloc[-10], rs_line.iloc[-10])
        
        # --- [3] 量能与收缩 (V20/VCP) ---
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean()) * 100
        avg_vol = vol.tail(20).mean()
        v_dry = bool(vol.iloc[-1] < avg_vol * 0.7) # 极度缩量
        
        # A. 老龙回头 (V20 Pullback)
        dist_ma50 = safe_div(abs(close.iloc[-1] - ma50.iloc[-1]), ma50.iloc[-1])
        is_dragon_back = bool(is_stage_2 and dist_ma50 < 0.025 and v_dry)
        
        # B. 口袋突破 (Pocket Pivot)
        max_dn_vol = vol[close < close.shift(1)].tail(10).max()
        is_pocket = bool(close.iloc[-1] > close.iloc[-2] and vol.iloc[-1] > max_dn_vol)
        
        # --- [4] 决策分流器 (核心逻辑升级) ---
        if is_dragon_back and rs_stealth: action = "🔥 双效共振(MAX)"
        elif rs_stealth: action = "👁️ 奇点先行(V130)"
        elif is_dragon_back: action = "🐉 老龙回头(V20)"
        elif is_pocket and tightness < 1.6: action = "🎯 口袋突破(V380)"
        elif is_stage_2 and rs_nh: action = "💎 趋势领袖(V100)"
        elif is_stage_2 and rs_accel > 0.02: action = "🚀 加速仰攻"
        else: action = "观察"
        
        # 综合评分：相对强度 + 宏观权重
        score = safe_div(close.iloc[-1], close.iloc[-126]) * (1.2 if dxy_down else 1.0)
        if "🔥" in action: score *= 1.4
        
        adr = ((high - low)/low).tail(20).mean()
        stop = close.iloc[-1] * (1 - adr * 2.0) # 动态 ADR 止损

        return {
            "score": score, "action": action, "tight": tightness, "price": close.iloc[-1],
            "stop": stop, "adr": adr * 100, "rs_nh": rs_nh, "dollar_vol": (vol.tail(5) * close.tail(5)).mean()
        }
    except: return None

# ==========================================
# 3. 混沌哨兵主流程
# ==========================================
def run_v500_chaos_sentinel():
    print(f"📡 [1/3] V500 混沌哨兵：探测全市场宏观气压...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 宏观审计
    macro = yf.download(["DX-Y.NYB", "^VIX", "SPY"], period="5d", progress=False)['Close']
    dxy_down = bool(macro["DX-Y.NYB"].iloc[-1] < macro["DX-Y.NYB"].iloc[0])
    vix = float(macro["^VIX"].iloc[-1])
    
    # 动态名册与行业映射
    try:
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        sp_list = sp_df['Symbol'].str.replace('.', '-').tolist()
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(set(sp_list + LEADER_POOL))
    except:
        tickers = LEADER_POOL; ticker_sector_map = {t: "Leaders" for t in tickers}

    data = yf.download(list(set(tickers + ["SPY"])), period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"]["Close"].dropna()

    # 市场宽度审计 (Breadth)
    valid_ts = [t for t in tickers if t in data.columns.levels[0]]
    breadth_c = 0
    for t in valid_ts[:250]:
        c = data[t]["Close"].dropna()
        if len(c) > 50 and c.iloc[-1] > c.tail(50).mean(): breadth_c += 1
    breadth = (breadth_c / 250) * 100 if len(valid_ts) > 0 else 50

    print(f"🚀 [2/3] 全天候策略机群演算 (大盘宽度: {breadth:.1f}%)...")
    candidates = []
    for t in valid_ts:
        try:
            df = data[t].dropna()
            if len(df) < 250: continue
            
            v500 = calculate_v500_engine(df, spy_df, dxy_down)
            if not v500 or v500['action'] == "观察": continue
            
            # --- 混沌风控规则 ---
            # 1. 极度恐慌市：封印突破，只留回踩
            if vix > 29 and "🐉" not in v500['action']: continue
            # 2. 垃圾时间：过滤掉 RS 弱的标的
            if breadth < 40 and v500['score'] < 1.1: continue

            candidates.append({
                "Ticker": t, "Action": v500['action'], "Score": v500['score'], 
                "Sector": ticker_sector_map.get(t, "Other"), "Price": v500['price'],
                "止损位": v500['stop'], "ADR%": v500['adr'], "紧致度": v500['tight'],
                "Stock_Dollar_Vol": v500['dollar_vol']
            })
        except: continue

    if not candidates: final_output([], vix, breadth, dxy_down); return
    
    # 行业平衡排序 (每个行业限 2 名)
    cand_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    final_seeds = cand_df.groupby("Sector").head(2).sort_values(by="Score", ascending=False).head(10)

    print(f"🔥 [3/3] 哨兵终审：期权真伪审计 (V/OI 穿透)...")
    results = []
    weather = "☀️ 极佳" if (breadth > 60 and vix < 21) else "⛈️ 严寒" if (breadth < 40 or vix > 29) else "☁️ 震荡"

    for _, row in final_seeds.iterrows():
        # UOA 深度异动审计
        uoa_status, call_pct, opt_vol = get_v500_option_intel(row['Ticker'])
        os_ratio = safe_div(opt_vol, row['Stock_Dollar_Vol'])
        
        # 财报倒计时
        try:
            t_obj = yf.Ticker(row['Ticker'])
            cal = t_obj.calendar
            e_str = cal.iloc[0, 0].date().strftime('%m-%d') if (cal is not None and not cal.empty) else "未知"
        except: e_str = "未知"

        row_dict = row.to_dict()
        row_dict.update({
            "财报日": e_str, "期权看涨%": call_pct, "期权异动": uoa_status, 
            "期现比": f"{round(os_ratio*100, 1)}%",
            "评级": "💎SSS" if (call_pct > 63 and "🔥" in uoa_status and weather != "⛈️ 严寒") else "🔥强势"
        })
        results.append(row_dict)
        time.sleep(12) # Polygon 免费版频率保护

    final_output(results, vix, breadth, dxy_down, weather)

def get_v500_option_intel(ticker):
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
            ["🏰 [V500 混沌哨兵 - 全天候指挥部]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["当前环境:", weather, "宽度(50MA):", f"{round(breadth, 1)}%", "VIX指数:", round(vix, 2), "美元顺风:", "✅" if dxy_down else "-"],
            ["作战指令:", "系统根据行情自动分配：🔥双效(最强)、🐉老龙(低吸)、👁️奇点(暗盘)、🚀加速(爆发)。"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "期权异动", "Price", "止损位", "期权看涨%", "期现比", "ADR%", "财报日", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            raw_data = [df.columns.tolist()] + df.values.tolist()
            clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            sh.update(values=clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "📭 全域演算完成：当前环境未探测到符合哨兵准则的混沌信号。")
        print(f"🎉 V500 指令下达成功。当前天气：{weather}")
    except Exception as e: print(f"❌ 最终写入失败: {e}")

if __name__ == "__main__":
    run_v500_chaos_sentinel()
