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

# 行业 ETF 映射表
SECTOR_MAP = {
    "Information Technology": "XLK", "Health Care": "XLV", "Financials": "XLF",
    "Consumer Discretionary": "XLY", "Communication Services": "XLC",
    "Industrials": "XLI", "Consumer Staples": "XLP", "Energy": "XLE",
    "Real Estate": "XLRE", "Materials": "XLB", "Utilities": "XLU"
}

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
# 2. V600 天枢核心引擎
# ==========================================
def calculate_v600_apex_engine(df, spy_df, sector_etf_df, dxy_down):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        
        # --- [1] 趋势对齐 (Stage 2) ---
        ma50, ma200 = close.rolling(50).mean(), close.rolling(200).mean()
        is_stage_2 = bool(close.iloc[-1] > ma50.iloc[-1] > ma200.iloc[-1])
        
        # --- [2] 双重强度验证 (Relative Strength vs SPY & Sector) ---
        rs_spy = (close / spy_df).fillna(method='ffill')
        rs_sector = (close / sector_etf_df).fillna(method='ffill')
        # 强于大盘且强于行业 ETF
        is_double_strong = bool(rs_spy.iloc[-1] > rs_spy.tail(20).mean() and rs_sector.iloc[-1] > rs_sector.tail(20).mean())
        rs_nh = bool(rs_spy.iloc[-1] >= rs_spy.tail(252).max())
        
        # --- [3] 阻力区与紧致度 ---
        dist_from_high = safe_div(df['High'].tail(252).max() - close.iloc[-1], close.iloc[-1])
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean()) * 100
        
        # --- [4] 量能异动 (Pocket Pivot / V-Dry) ---
        avg_vol = vol.tail(20).mean()
        v_dry = bool(vol.iloc[-1] < avg_vol * 0.7)
        max_dn_vol = vol[close < close.shift(1)].tail(10).max()
        is_pocket = bool(close.iloc[-1] > close.iloc[-2] and vol.iloc[-1] > max_dn_vol)
        
        # --- [5] 动作逻辑标记 ---
        if rs_nh and dist_from_high < 0.03: action = "👁️ 奇点先行(V600)"
        elif is_stage_2 and dist_from_high < 0.02 and v_dry: action = "🐉 老龙回头"
        elif is_pocket and tightness < 1.5: action = "🚀 口袋爆发"
        elif is_stage_2 and is_double_strong: action = "💎 行业领袖"
        else: action = "观察"
        
        # --- [6] 风险头寸计算 ---
        adr = ((high - low)/low).tail(20).mean()
        atr = (high - low).rolling(14).mean().iloc[-1]
        stop_price = close.iloc[-1] - (2.2 * atr)
        risk_per_share = close.iloc[-1] - stop_loss_price if (close.iloc[-1] - stop_price) > 0 else (close.iloc[-1] * 0.05)
        # 每笔交易风险账户 1% 的头寸建议
        suggested_shares = math.floor((ACCOUNT_SIZE * 0.01) / (close.iloc[-1] - stop_price)) if (close.iloc[-1] - stop_price) > 0 else 0

        score = (rs_spy.iloc[-1]/rs_spy.iloc[-63]) * (1.3 if is_double_strong else 1.0) * (1.2 if dxy_down else 1.0)
        
        return {
            "score": score, "action": action, "tight": tightness, "price": close.iloc[-1],
            "stop": stop_price, "shares": suggested_shares, "adr": adr * 100, "rs_nh": rs_nh,
            "dollar_vol": (vol.tail(5) * close.tail(5)).mean(), "double_strong": is_double_strong
        }
    except: return None

# ==========================================
# 3. 巅峰指挥流程
# ==========================================
def run_v600_apex_command():
    print(f"📡 [1/3] V600 天枢巅峰：同步全市场行业 ETF 与宏观共振...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 宏观与行业 ETF 下载
    macro_symbols = ["SPY", "DX-Y.NYB", "^VIX"] + list(SECTOR_MAP.values())
    m_data = yf.download(macro_symbols, period="2y", progress=False)['Close']
    spy_df = m_data["SPY"].dropna()
    dxy_down = bool(m_data["DX-Y.NYB"].iloc[-1] < m_data["DX-Y.NYB"].iloc[0])
    vix = float(m_data["^VIX"].iloc[-1])

    # 名册审计
    try:
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(ticker_sector_map.keys()) + ["NVDA", "GOOGL", "CF", "PR", "PLTR", "TSLA"]
    except:
        tickers = ["NVDA", "GOOGL", "CF", "PR"]; ticker_sector_map = {}

    data = yf.download(tickers, period="2y", group_by='ticker', threads=True, progress=False)

    print(f"🚀 [2/3] 执行天枢机群演算 (头寸基准: ${ACCOUNT_SIZE})...")
    candidates = []
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 200: continue
            
            sector_name = ticker_sector_map.get(t, "Other")
            sector_etf_ticker = SECTOR_MAP.get(sector_name, "SPY")
            sector_etf_df = m_data[sector_etf_ticker].dropna() if sector_etf_ticker in m_data.columns else spy_df
            
            v600 = calculate_v600_apex_engine(df, spy_df, sector_etf_df, dxy_down)
            if not v600 or v600['action'] == "观察": continue
            
            # 环境熔断：VIX极高时只保留防守性强的动作
            if vix > 30 and v600['action'] != "🐉 老龙回头": continue

            candidates.append({
                "Ticker": t, "Action": v600['action'], "Score": v600['score'], 
                "Sector": sector_name, "Price": v600['price'],
                "建议买入": v600['shares'], "止损位": v600['stop'], 
                "ADR%": v600['adr'], "行业领袖": "💎" if v600['double_strong'] else "-",
                "Stock_Dollar_Vol": v600['dollar_vol']
            })
        except: continue

    if not candidates: final_output([], vix, dxy_down); return
    
    # 行业配额与评分排序
    cand_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    final_seeds = cand_df.groupby("Sector").head(2).sort_values(by="Score", ascending=False).head(10)

    print(f"🔥 [3/3] 哨兵审计：机构期权头寸穿透...")
    results = []
    for _, row in final_seeds.iterrows():
        uoa_status, call_pct, opt_vol = get_v600_option_intel(row['Ticker'])
        os_ratio = safe_div(opt_vol, row['Stock_Dollar_Vol'])
        
        try:
            t_obj = yf.Ticker(row['Ticker'])
            cal = t_obj.calendar
            e_str = cal.iloc[0, 0].date().strftime('%m-%d') if (cal is not None and not cal.empty) else "未知"
        except: e_str = "未知"

        row_dict = row.to_dict()
        row_dict.update({
            "财报日": e_str, "期权看涨%": call_pct, "期权异动": uoa_status, 
            "期现比": f"{round(os_ratio*100, 1)}%",
            "评级": "💎SSS" if (call_pct > 64 and "🔥" in uoa_status and vix < 24) else "🔥强势"
        })
        results.append(row_dict)
        time.sleep(12) 

    final_output(results, vix, dxy_down)

def get_v600_option_intel(ticker):
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
        
        header = [
            ["🏰 [V600 天枢巅峰 - 机构头寸版]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["宏观环境:", "顺风" if dxy_down else "逆风", "VIX指数:", round(vix, 2), "美元顺风:", "✅" if dxy_down else "-"],
            ["作战指令:", "系统已根据 ATR 止损距离计算建议买入股数(基于$10k账户)。"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "建议买入", "止损位", "行业领袖", "期权异动", "Price", "期权看涨%", "期现比", "ADR%", "财报日", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            raw_data = [df.columns.tolist()] + df.values.tolist()
            clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            sh.update(values=clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "📭 天枢演算完成：当前未探测到符合 Apex 机构级准则的信号。")
        print(f"🎉 V600 任务成功。VIX: {vix}")
    except Exception as e: print(f"❌ 最终写入失败: {e}")

if __name__ == "__main__":
    run_v600_apex_command()
