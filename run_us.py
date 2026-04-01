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
ACCOUNT_SIZE = 10000  # 建议头寸计算基准

# 领袖核心池 (强制审计)
CORE_LEADERS = ["NVDA", "GOOGL", "CF", "PR", "TSLA", "PLTR", "META", "AVGO", "COST", "AAPL", "MSFT", "AMZN"]

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
# 2. V750 阿尔法核心引擎 (Minervini + O'Neil)
# ==========================================
def calculate_v750_apex_engine(df, spy_df):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        
        # 1. Minervini 趋势模板 (Stage 2)
        ma50, ma150, ma200 = close.rolling(50).mean(), close.rolling(150).mean(), close.rolling(200).mean()
        # 核心：股价 > MA50 > MA150 > MA200 且 MA200 趋势向上
        is_stage_2 = bool(close.iloc[-1] > ma50.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1] and ma200.iloc[-1] > ma200.iloc[-20])
        
        # 2. RS 巅峰动量 (IBD 模拟评分)
        rs_line = (close / spy_df).fillna(method='ffill')
        rs_3m = safe_div(close.iloc[-1], close.iloc[-63])
        rs_6m = safe_div(close.iloc[-1], close.iloc[-126])
        rs_12m = safe_div(close.iloc[-1], close.iloc[-252])
        # RS加权分：近3个月表现最重要
        rs_score = (rs_3m * 2) + rs_6m + rs_12m
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(252).max()) # RS线创一年新高
        
        # 3. VCP 紧致度 (收缩判定)
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean()) * 100
        
        # 4. U/D 成交量积累比 (过去 50 天机构脚印)
        up_vol = vol[close > close.shift(1)].tail(50).sum()
        dn_vol = vol[close < close.shift(1)].tail(50).sum()
        ud_ratio = safe_div(up_vol, dn_vol)
        
        # 5. 奇点判定 (RS Stealth)
        # 股价还没突破 20 日高点，但 RS 线已经创新高
        rs_stealth = bool(rs_nh and close.iloc[-1] < close.tail(20).max() * 1.02)
        
        # 6. 老龙回头与量能枯竭 (V-Dry)
        dist_ma50 = safe_div(abs(close.iloc[-1] - ma50.iloc[-1]), ma50.iloc[-1])
        v_dry = bool(vol.iloc[-1] < vol.tail(10).mean() * 0.7)
        
        # --- 策略决策标签 ---
        if rs_stealth and tightness < 1.2: action = "👁️ 奇点先行(RS Stealth)"
        elif is_stage_2 and dist_ma50 < 0.025 and v_dry: action = "🐉 老龙回头(V-Dry)"
        elif rs_nh and close.iloc[-1] >= close.tail(252).max(): action = "🚀 动量爆发(Breakout)"
        elif is_stage_2 and rs_nh: action = "💎 双重共振(Leader)"
        else: action = "观察"
        
        # --- 止损与头寸规模 ---
        adr = ((high - low)/low).tail(20).mean() # 平均日波幅
        stop_price = close.iloc[-1] * (1 - adr * 1.8) # 1.8倍ADR止损，既给空间又不死扛
        risk_per_share = close.iloc[-1] - stop_price
        shares = math.floor((ACCOUNT_SIZE * 0.01) / risk_per_share) if risk_per_share > 0 else 0

        return {
            "score": rs_score, "action": action, "tight": tightness, "price": close.iloc[-1],
            "stop": stop_price, "shares": shares, "ud": ud_ratio, "rs_nh": rs_nh,
            "adr": adr * 100, "dollar_vol": (vol.tail(5) * close.tail(5)).mean(), "is_stage_2": is_stage_2
        }
    except: return None

# ==========================================
# 3. 自动化扫描引擎
# ==========================================
def run_v750_apex_sentinel():
    print(f"📡 [1/3] V750 巅峰指挥部：正在执行全域动量探测...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 宏观审计
    macro = yf.download(["SPY", "^VIX", "DX-Y.NYB"], period="5d", progress=False)['Close']
    vix = float(macro["^VIX"].iloc[-1])
    dxy_down = bool(macro["DX-Y.NYB"].iloc[-1] < macro["DX-Y.NYB"].iloc[0])
    
    # 获取名册 (双层保障机制)
    try:
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        sp_list = sp_df['Symbol'].str.replace('.', '-').tolist()
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(set(sp_list + CORE_LEADERS))
    except:
        tickers = CORE_LEADERS; ticker_sector_map = {t: "Leaders" for t in tickers}

    data = yf.download(list(set(tickers + ["SPY"])), period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"]["Close"].dropna()

    # 市场宽度采样 (250只)
    valid_ts = [t for t in tickers if t in data.columns.levels[0]]
    breadth_c = 0
    for t in valid_ts[:250]:
        c = data[t]["Close"].dropna()
        if len(c) > 50 and c.iloc[-1] > c.tail(50).mean(): breadth_c += 1
    breadth = (breadth_c / 250) * 100 if len(valid_ts) > 0 else 50

    print(f"🚀 [2/3] 执行大师级形态审计 (当前 VIX: {vix:.2f} / 宽度: {breadth:.1f}%)...")
    candidates = []
    for t in valid_ts:
        try:
            df = data[t].dropna()
            if len(df) < 252: continue
            
            v750 = calculate_v750_apex_engine(df, spy_df)
            if not v750 or v750['action'] == "观察": continue
            
            # 环境风控：VIX极高时强制封锁突破信号
            if vix > 29 and "🚀" in v750['action']: continue
            # 必须在 Stage 2
            if not v750['is_stage_2']: continue

            candidates.append({
                "Ticker": t, "Action": v750['action'], "Score": v750['score'], 
                "Sector": ticker_sector_map.get(t, "Other"), "Price": v750['price'],
                "建议买入": v750['shares'], "止损位": v750['stop'], 
                "U/D比": v750['ud'], "紧致度": v750['tight'], "ADR%": v750['adr'],
                "RS新高": "🌟" if v750['rs_nh'] else "-", "Stock_Dollar_Vol": v750['dollar_vol']
            })
        except: continue

    if not candidates: final_output([], vix, breadth); return
    
    # 行业配额筛选
    cand_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    final_seeds = cand_df.groupby("Sector").head(2).sort_values(by="Score", ascending=False).head(10)

    print(f"🔥 [3/3] 正在对领袖候选人进行期权异动审计...")
    results = []
    weather = "☀️ 极佳" if (breadth > 60 and vix < 21) else "⛈️ 风险" if (breadth < 40 or vix > 28) else "☁️ 震荡"

    for _, row in final_seeds.iterrows():
        # 期权异动 (V/OI 穿透)
        uoa_status, call_pct, opt_vol = get_apex_uoa_intel(row['Ticker'])
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
            "评级": "💎SSS" if (call_pct > 64 and "🔥" in uoa_status and "👁️" in row['Action']) else "🔥强势"
        })
        results.append(row_dict)
        time.sleep(12.5) 

    final_output(results, vix, breadth, weather)

def get_apex_uoa_intel(ticker):
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
        
        status = "🔥主力扫货" if max_v_oi > 1.5 else "⚠️放量" if max_v_oi > 0.8 else "平稳"
        return status, round(safe_div(call_val, total_val)*100, 1), total_val
    except: return "N/A", 50.0, 0.0

def final_output(res, vix, breadth, weather):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        # 🟢 强制定向：转换为北京时间 (UTC+8)
        beijing_tz = datetime.timezone(datetime.timedelta(hours=8))
        bj_time_str = datetime.datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M')

        header = [
            ["🏰 [V750 哨兵巅峰 - 大师形态版]", "", "Update(北京时间):", bj_time_str],
            ["当前天气:", weather, "宽度(50MA):", f"{round(breadth, 1)}%", "VIX指数:", round(vix, 2)],
            ["大师指令:", "关注【👁️奇点先行】感知机构偷跑，关注【🐉老龙回头】捕捉极致缩量买点。"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "期权异动", "Price", "建议买入", "止损位", "U/D比", "紧致度", "期权看涨%", "期现比", "财报日", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            raw_data = [df.columns.tolist()] + df.values.tolist()
            clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            sh.update(values=clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "📭 全域审计完成：当前环境未探测到符合 V750 大师形态的信号。")
        print(f"🎉 V750 巅峰指令下达！状态：{weather} (表格时间已更新为: {bj_time_str})")
    except Exception as e: print(f"❌ 最终写入失败: {e}")

if __name__ == "__main__":
    run_v750_apex_sentinel()
