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
ACCOUNT_SIZE = 10000  # 建议头寸计算基准：1万美元

SECTOR_MAP = {
    "Information Technology": "XLK", "Health Care": "XLV", "Financials": "XLF",
    "Consumer Discretionary": "XLY", "Communication Services": "XLC",
    "Industrials": "XLI", "Consumer Staples": "XLP", "Energy": "XLE",
    "Real Estate": "XLRE", "Materials": "XLB", "Utilities": "XLU"
}

# ==========================================
# 🛡️ 核心工具：数据净化 (解决 Series Ambiguous 报错)
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
# 2. V650 核心演算逻辑
# ==========================================
def calculate_v650_resilience_engine(df, spy_df, sector_etf_df, dxy_down, vix):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        
        # 1. 均线对齐与多头结构
        ma50, ma150, ma200 = close.rolling(50).mean(), close.rolling(150).mean(), close.rolling(200).mean()
        is_stage_2 = bool(close.iloc[-1] > ma50.iloc[-1] > ma200.iloc[-1])
        
        # 2. 相对强度监控
        rs_spy = (close / spy_df).fillna(method='ffill')
        rs_sector = (close / sector_etf_df).fillna(method='ffill')
        is_double_strong = bool(rs_spy.iloc[-1] > rs_spy.tail(20).mean() and rs_sector.iloc[-1] > rs_sector.tail(20).mean())
        
        # 3. 恐慌韧性判定 (VIX > 28 时的核心指标)
        # 跌幅对比：最近5天个股表现 vs 大盘表现
        stock_5d_ret = safe_div(close.iloc[-1] - close.iloc[-5], close.iloc[-5])
        spy_5d_ret = safe_div(spy_df.iloc[-1] - spy_df.iloc[-5], spy_df.iloc[-5])
        is_resilient = bool(stock_5d_ret > spy_5d_ret and close.iloc[-1] > ma50.iloc[-1])
        
        # 4. 紧致度与量能
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean()) * 100
        avg_vol = vol.tail(20).mean()
        v_dry = bool(vol.iloc[-1] < avg_vol * 0.75)
        
        # --- 动作标记逻辑 ---
        if vix > 28:
            if is_resilient and is_double_strong: action = "🛡️ 恐慌锚点(Resilient)"
            elif is_stage_2 and v_dry: action = "🐉 老龙回头"
            else: action = "观察"
        else:
            if rs_spy.iloc[-1] >= rs_spy.tail(252).max() and tightness < 1.5: action = "👁️ 奇点先行"
            elif is_double_strong and is_stage_2: action = "💎 行业领袖"
            else: action = "观察"
            
        # --- 风控与头寸计算 ---
        atr = (high - low).rolling(14).mean().iloc[-1]
        stop_price = close.iloc[-1] - (2.3 * atr)
        risk_per_share = close.iloc[-1] - stop_price
        
        # 计算建议股数 (每笔交易风险 1% 的账户资金)
        suggested_shares = math.floor((ACCOUNT_SIZE * 0.01) / risk_per_share) if risk_per_share > 0 else 0
        
        score = safe_div(close.iloc[-1], close.iloc[-63]) * (1.2 if dxy_down else 1.0)
        
        return {
            "score": score, "action": action, "price": close.iloc[-1],
            "stop": stop_price, "shares": suggested_shares, "adr": safe_div(high.iloc[-1]-low.iloc[-1], low.iloc[-1])*100,
            "resilience": is_resilient, "dollar_vol": (vol.tail(5) * close.tail(5)).mean()
        }
    except: return None

# ==========================================
# 3. 自动化流程
# ==========================================
def run_v650_resilience():
    print(f"📡 [1/3] V650 启动：同步宏观背景与行业 ETF...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    macro_symbols = ["SPY", "DX-Y.NYB", "^VIX"] + list(SECTOR_MAP.values())
    m_data = yf.download(macro_symbols, period="2y", progress=False)['Close']
    spy_df = m_data["SPY"].dropna()
    dxy_down = bool(m_data["DX-Y.NYB"].iloc[-1] < m_data["DX-Y.NYB"].iloc[0])
    vix = float(m_data["^VIX"].iloc[-1])

    try:
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(ticker_sector_map.keys()) + ["NVDA", "GOOGL", "CF", "PR", "PLTR", "TSLA"]
    except:
        tickers = ["NVDA", "GOOGL", "CF", "PR"]; ticker_sector_map = {}

    data = yf.download(tickers, period="2y", group_by='ticker', threads=True, progress=False)

    print(f"🚀 [2/3] 执行韧性演算 (当前 VIX: {vix:.2f})...")
    candidates = []
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 200: continue
            
            sector_name = ticker_sector_map.get(t, "Other")
            sector_etf_df = m_data[SECTOR_MAP.get(sector_name, "SPY")].dropna()
            
            v650 = calculate_v650_resilience_engine(df, spy_df, sector_etf_df, dxy_down, vix)
            if not v650 or v650['action'] == "观察": continue

            candidates.append({
                "Ticker": t, "Action": v650['action'], "Score": v650['score'], 
                "Sector": sector_name, "Price": v650['price'],
                "建议买入": v650['shares'], "止损位": v650['stop'], 
                "ADR%": v650['adr'], "抗跌": "✅" if v650['resilience'] else "-",
                "Stock_Dollar_Vol": v650['dollar_vol']
            })
        except: continue

    if not candidates: final_output([], vix, dxy_down); return
    
    cand_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    final_seeds = cand_df.groupby("Sector").head(2).sort_values(by="Score", ascending=False).head(10)

    print(f"🔥 [3/3] 审计异动头寸审计...")
    results = []
    for _, row in final_seeds.iterrows():
        uoa_status, call_pct, opt_vol = get_v650_option_intel(row['Ticker'])
        os_ratio = safe_div(opt_vol, row['Stock_Dollar_Vol'])
        
        try:
            t_obj = yf.Ticker(row['Ticker'])
            cal = t_obj.calendar
            e_str = cal.iloc[0, 0].date().strftime('%m-%d') if (cal is not None and not cal.empty) else "未知"
        except: e_str = "未知"

        row_dict = row.to_dict()
        row_dict.update({
            "财报日": e_str, "期权看涨%": call_pct, "异动监控": uoa_status, 
            "期现比": f"{round(os_ratio*100, 1)}%",
            "评级": "💎SSS" if (call_pct > 64 and vix < 26) else "🔥强势"
        })
        results.append(row_dict)
        time.sleep(12) 

    final_output(results, vix, dxy_down)

def get_v650_option_intel(ticker):
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
        
        status = "🔥主力扫货" if max_v_oi > 1.4 else "⚠️放量" if max_v_oi > 0.9 else "平稳"
        return status, round(safe_div(call_val, total_val)*100, 1), total_val
    except: return "N/A", 50.0, 0.0

def final_output(res, vix, dxy_down):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        status = "⛈️ 恐慌避险" if vix > 30 else "☁️ 震荡防御" if vix > 22 else "☀️ 进攻多头"
        header = [
            [robust_json_clean("🏰 [V650 哨兵版 - 抗跌韧性系统]"), "", robust_json_clean("Update:"), robust_json_clean(datetime.datetime.now().strftime('%Y-%m-%d %H:%M'))],
            [robust_json_clean("市场状态:"), robust_json_clean(status), robust_json_clean("VIX指数:"), robust_json_clean(vix)],
            [robust_json_clean("作战指令:"), robust_json_clean("VIX极高时，系统自动切换为【恐慌锚点】模式，寻找真正抗跌的核心资产。")],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "建议买入", "止损位", "抗跌", "期权异动", "Price", "期权看涨%", "期现比", "ADR%", "财报日", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            raw_data = [df.columns.tolist()] + df.values.tolist()
            clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            sh.update(values=clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "📭 恐慌审计完成：当前环境未探测到具备阿尔法韧性的避险标的。")
        print(f"🎉 V650 任务成功。VIX: {vix:.2f}")
    except Exception as e: print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    run_v650_resilience()
