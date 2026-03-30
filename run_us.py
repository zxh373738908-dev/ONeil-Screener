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
# 🛡️ 核心工具：终极净化 (100% 兼容 Google Sheets)
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
# 2. V300 泰坦演算引擎 (Unified Alpha Engine)
# ==========================================
def calculate_v300_metrics(df, spy_df, dxy_down):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        
        # --- [1] 趋势模板 (V100/V200 核心) ---
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()
        is_stage_2 = bool(close.iloc[-1] > ma50.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1])
        
        # --- [2] 相对强度 & 奇点 (V130 核心) ---
        rs_line = (close / spy_df).fillna(method='ffill')
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(252).max())
        rs_stealth = bool(rs_nh and close.iloc[-1] < close.tail(20).max())
        
        # --- [3] 战术动作识别 (V20/V130 混合) ---
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean()) * 100
        avg_vol = vol.tail(20).mean()
        v_dry = bool(vol.iloc[-1] < avg_vol * 0.7)
        
        # A. 老龙回头: 趋势强 + 回踩50MA + 极度缩量
        dist_ma50 = safe_div(abs(close.iloc[-1] - ma50.iloc[-1]), ma50.iloc[-1])
        is_pullback = bool(is_stage_2 and dist_ma50 < 0.02 and v_dry)
        
        # B. 口袋突破: 基底内量能异动 (今日量 > 过去10天最大下跌日量)
        max_dn_vol = vol[close < close.shift(1)].tail(10).max()
        is_pocket = bool(close.iloc[-1] > close.iloc[-2] and vol.iloc[-1] > (max_dn_vol if not pd.isna(max_dn_vol) else 0))
        
        # --- [4] 评分逻辑 ---
        # 基础分：由RS强度决定
        base_score = safe_div(close.iloc[-1], close.iloc[-126]) * 100 
        # 宏观加成：若美元走弱，资源/权重股加分
        macro_bonus = 1.2 if dxy_down else 1.0
        
        # 指令优先级判定
        if rs_stealth and tightness < 1.0: action = "👁️奇点先行(V130)"
        elif is_pullback: action = "🐉老龙回头(V20)"
        elif is_pocket and tightness < 1.5: action = "🎯口袋突破(V20)"
        elif is_stage_2 and rs_nh: action = "💎双重共振(V200)"
        elif is_stage_2: action = "👑领袖锁仓"
        else: action = "观察"

        atr = (high - low).rolling(14).mean().iloc[-1]
        stop = close.iloc[-1] - (2.5 * atr)
        
        return {
            "score": base_score * macro_bonus, "action": action, "is_stage_2": is_stage_2,
            "rs_nh": rs_nh, "tight": tightness, "price": close.iloc[-1],
            "stop": stop, "dollar_vol": (vol.tail(5) * close.tail(5)).mean()
        }
    except: return None

# ==========================================
# 3. 主指挥系统
# ==========================================
def run_v300_omni_titan():
    print("🏟️ [1/3] V300 泰坦指挥部：同步全球宏观因子与宽度...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 宏观数据
    macro = yf.download(["DX-Y.NYB", "^VIX", "SPY"], period="5d", progress=False)['Close']
    dxy_down = bool(macro["DX-Y.NYB"].iloc[-1] < macro["DX-Y.NYB"].iloc[0])
    vix = float(macro["^VIX"].iloc[-1])
    
    # 标的名册
    try:
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(ticker_sector_map.keys()) + ["NVDA", "GOOGL", "CF", "PR", "PLTR", "TSLA"]
    except:
        tickers = ["NVDA", "GOOGL", "AAPL", "MSFT", "CF", "PR"]

    data = yf.download(list(set(tickers + ["SPY"])), period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"]["Close"].dropna()

    # 市场宽度采样
    above_50ma = 0
    valid_count = 0
    for t in tickers[:200]:
        if t in data.columns.levels[0]:
            c = data[t]["Close"].dropna()
            if len(c) > 50 and c.iloc[-1] > c.tail(50).mean(): above_50ma += 1
            valid_count += 1
    breadth = (above_50ma / valid_count) * 100 if valid_count > 0 else 50

    print(f"🚀 [2/3] 执行全域阿尔法演算 (宽度: {breadth:.1f}% / 美元顺风: {dxy_down})...")
    candidates = []
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 200: continue
            
            v300 = calculate_v300_metrics(df, spy_df, dxy_down)
            if not v300 or v300['action'] == "观察": continue
            
            # 环境过滤：VIX高时只看老龙回头或Stage2
            if vix > 26 and "🐉" not in v300['action']: continue

            candidates.append({
                "Ticker": t, "Action": v300['action'], "Score": v300['score'], 
                "Sector": ticker_sector_map.get(t, "Other"), "Price": v300['price'],
                "止损位": v300['stop'], "紧致度": v300['tight'],
                "Stock_Dollar_Vol": v300['dollar_vol']
            })
        except: continue

    if not candidates: final_output([], vix, breadth, dxy_down); return
    
    # 行业配额 + 评分排序
    cand_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    final_seeds = cand_df.groupby("Sector").head(2).sort_values(by="Score", ascending=False).head(10)

    print(f"🔥 [3/3] 哨兵审计：期权异动 (V/OI) 与财报核验...")
    results = []
    weather = "☀️ 极佳" if (breadth > 60 and vix < 21) else "⛈️ 风险" if (breadth < 40 or vix > 28) else "☁️ 震荡"

    for _, row in final_seeds.iterrows():
        # 财报日审计
        try:
            t_obj = yf.Ticker(row['Ticker'])
            cal = t_obj.calendar
            e_str = cal.iloc[0, 0].date().strftime('%m-%d') if (cal is not None and not cal.empty) else "未知"
        except: e_str = "未知"

        # 期权审计 (UOA 逻辑)
        uoa_status, call_pct, opt_vol = get_v300_option_intel(row['Ticker'])
        os_ratio = safe_div(opt_vol, row['Stock_Dollar_Vol'])
        
        row_dict = row.to_dict()
        row_dict.update({
            "财报日": e_str, "期权看涨%": call_pct, "异动监控": uoa_status, 
            "期现比": f"{round(os_ratio*100, 1)}%",
            "评级": "💎SSS" if (call_pct > 62 and "🔥" in uoa_status and weather != "⛈️ 风险") else "🔥强势"
        })
        results.append(row_dict)
        time.sleep(12.5) 

    final_output(results, vix, breadth, dxy_down, weather)

def get_v300_option_intel(ticker):
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
        
        status = "🔥主力扫货" if max_v_oi > 1.3 else "⚠️异常放量" if max_v_oi > 0.8 else "平稳"
        return status, round(safe_div(call_val, total_val)*100, 1), total_val
    except: return "N/A", 50.0, 0.0

def final_output(res, vix, breadth, dxy_down, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        header = [
            ["🏟️ [V300 泰坦全域指挥部 - Titan Omni]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["环境天气:", weather, "大盘宽度:", f"{round(breadth, 1)}%", "VIX:", round(vix, 2), "美元顺风:", "✅" if dxy_down else "-"],
            ["作战指令:", "防守市盯紧【🐉老龙回头】，进攻市锁定【👁️奇点先行】或【🎯口袋突破】。"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "异动监控", "Price", "止损位", "期权看涨%", "期现比", "紧致度", "财报日", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            raw_data = [df.columns.tolist()] + df.values.tolist()
            clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            sh.update(values=clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "📭 当前全域演算未发现符合泰坦准则的标的。")
            
        print(f"🎉 V300 任务完成！大盘状态：{weather}")
    except Exception as e: print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    run_v300_omni_titan()
