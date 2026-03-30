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

SECTOR_ETF = {
    "Information Technology": "XLK", "Health Care": "XLV", "Financials": "XLF",
    "Consumer Discretionary": "XLY", "Communication Services": "XLC",
    "Industrials": "XLI", "Consumer Staples": "XLP", "Energy": "XLE",
    "Real Estate": "XLRE", "Materials": "XLB", "Utilities": "XLU"
}

# ==========================================
# 2. 核心脱壳工具
# ==========================================
def robust_json_clean(val):
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)) or pd.isna(val): return ""
        if hasattr(val, 'item'): val = val.item()
        if isinstance(val, (float, int, np.floating, np.integer)):
            if not math.isfinite(val): return 0.0
            return float(round(val, 3)) if isinstance(val, float) else int(val)
        return str(val)
    except: return ""

def safe_div(n, d):
    try:
        n_f, d_f = float(n), float(d)
        return n_f / d_f if d_f != 0 and math.isfinite(n_f) and math.isfinite(d_f) else 0.0
    except: return 0.0

# ==========================================
# 3. V91 哨兵算法引擎
# ==========================================
def calculate_v91_metrics(df, spy_df, sector_df):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        
        # 1. 波动率挤压识别
        ma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        upper_bb = ma20 + (2 * std20)
        lower_bb = ma20 - (2 * std20)
        
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        atr20 = tr.rolling(20).mean()
        upper_kc = ma20 + (1.5 * atr20)
        lower_kc = ma20 - (1.5 * atr20)
        
        is_squeezing = upper_bb.iloc[-1] < upper_kc.iloc[-1] and lower_bb.iloc[-1] > lower_kc.iloc[-1]
        
        # 2. 强度校验
        rs_spy = (close / spy_df).fillna(method='ffill')
        rs_nh = rs_spy.iloc[-1] >= rs_spy.tail(30).max()
        rvol = safe_div(vol.iloc[-1], vol.tail(20).mean())
        
        # 3. 盈亏比与目标
        current_price = close.iloc[-1]
        adr_val = ( (high - low) / low ).tail(20).mean()
        stop_loss = current_price - (1.5 * atr20.iloc[-1])
        target_price = current_price + (current_price * adr_val * 2.5)
        
        risk = current_price - stop_loss
        reward = target_price - current_price
        rr_ratio = safe_div(reward, risk)
        
        # 4. 最终评分
        score = (2.0 if rs_nh else 1.0) + (1.5 if is_squeezing else 0) + (rvol * 0.5)
        
        return {
            "Score": score, "rr_ratio": rr_ratio, "is_squeeze": "🔥挤压中" if is_squeezing else "释放",
            "rvol": rvol, "stop": stop_loss, "target": target_price, "adr": adr_val * 100,
            "rs_nh": rs_nh, "extension": safe_div(current_price - ma20.iloc[-1], ma20.iloc[-1]) * 100
        }
    except: return None

# ==========================================
# 4. 自动化主流程
# ==========================================
def run_v91_sentinel():
    print(f"📡 [1/4] V9.1 哨兵启动：正在同步全市场数据...")
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(ticker_sector_map.keys()) + ["NVDA", "GOOGL", "CF", "PR", "TSLA", "MSFT"]
    except Exception as e:
        print(f"❌ 无法加载标的名册: {e}"); return

    all_symbols = list(set(tickers + ["SPY"] + list(SECTOR_ETF.values())))
    data = yf.download(all_symbols, period="1y", group_by='ticker', threads=True, progress=False)
    
    if data.empty:
        print("❌ 未下载到任何行情数据"); return

    spy_df = data["SPY"]["Close"].dropna()
    vix_data = yf.download("^VIX", period="5d", progress=False)['Close']
    vix = vix_data.iloc[-1] if not vix_data.empty else 20.0

    print(f"🚀 [2/4] 执行阿尔法演算 (严苛模式)...")
    pre_candidates = []
    
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 60: continue
            
            sector_name = ticker_sector_map.get(t, "Other")
            sector_df = data[SECTOR_ETF.get(sector_name, "SPY")]["Close"].dropna()
            
            v91 = calculate_v91_metrics(df, spy_df, sector_df)
            if not v91: continue
            
            # --- V9.1 严苛过滤逻辑 ---
            if v91['rr_ratio'] < 1.6: continue      # 盈亏比稍微放宽至 1.6 增加信号量
            if v91['extension'] > 7.5: continue     # 乖离放宽至 7.5
            if v91['rvol'] < 1.05: continue         # 量能异动
            if not v91['rs_nh']: continue           # 必须相对强度新高
            
            # 财报检查
            try:
                t_obj = yf.Ticker(t)
                cal = t_obj.calendar
                days_to_earn = (cal.iloc[0, 0].date() - datetime.date.today()).days if (cal is not None and not cal.empty) else 99
            except: days_to_earn = 99
            if 0 <= days_to_earn <= 3: continue 

            pre_candidates.append({
                "Ticker": t, "Sector": sector_name, "Score": v91['Score'],
                "Price": float(df['Close'].iloc[-1]), "R:R": v91['rr_ratio'],
                "状态": v91['is_squeeze'], "止损位": v91['stop'], "目标位": v91['target'],
                "RVOL": v91['rvol'], "ADR": v91['adr'], "财报": days_to_earn
            })
        except: continue

    # --- 关键修复：检查是否有结果 ---
    if not pre_candidates:
        print("📭 今日市场无符合‘哨兵阿尔法’条件的标的。")
        final_output([], vix)
        return

    final_seeds = pd.DataFrame(pre_candidates).sort_values(by="Score", ascending=False).head(6)

    print(f"🔥 [3/4] 期权暗盘审计 (Polygon Limit)...")
    results = []
    for _, row in final_seeds.iterrows():
        print(f"  审计 {row['Ticker']}...")
        opt_score, opt_size = get_sentiment(row['Ticker'])
        row['期权看涨%'] = opt_score
        row['大单规模'] = opt_size
        row['评级'] = "💎SSS" if (opt_score > 60 and row['状态'] == "🔥挤压中") else "🔥强势"
        results.append(row.to_dict())
        time.sleep(12) 

    final_output(results, vix)

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

def final_output(res, vix):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        header = [
            ["🏰 [V9.1 哨兵阿尔法 - 健壮修复版]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["VIX指数:", round(vix, 2), "系统环境:", "☀️ 激进" if vix < 21 else "⛈️ 防御"],
            ["运行状态:", "扫描完成。如果下方无数据，说明今日无符合高胜率条件的标的。"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "状态", "Price", "止损位", "目标位", "R:R", "RVOL", "ADR", "财报", "期权看涨%", "大单规模", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            raw_data = [df.columns.tolist()] + df.values.tolist()
            clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            sh.update(values=clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "📭 今日信号：零符合。建议持币观望或检查市场宽度。")
            
        print("🎉 V9.1 任务安全结束。")
    except Exception as e:
        print(f"❌ 最终写入失败: {e}")

if __name__ == "__main__":
    run_v91_sentinel()
