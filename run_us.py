import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import time
import math
import traceback
from polygon import RESTClient

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
POLYGON_API_KEY = "您的_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
creds_file = "credentials.json"
ACCOUNT_SIZE = 10000  

CORE_LEADERS = ["NVDA", "GOOGL", "CF", "PR", "TSLA", "PLTR", "META", "AVGO", "COST", "AAPL", "MSFT", "AMZN"]

# ==========================================
# 🛡️ 核心工具
# ==========================================
def robust_json_clean(val):
    try:
        if isinstance(val, (pd.Series, np.ndarray)): val = val.item() if val.size == 1 else str(val.tolist())
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
# 2. V750 阿尔法核心引擎 (加入流动性护城河)
# ==========================================
def calculate_v750_apex_engine(df, spy_df, spy_is_healthy):
    try:
        close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
        
        # 🟢 1. 机构级流动性与价格护城河 (前置拦截，加速运算)
        current_price = close.iloc[-1]
        dollar_vol = (vol.tail(5) * close.tail(5)).mean()
        
        if current_price < 10.0 or dollar_vol < 10_000_000:
            return None # 剔除毛票和缺乏流动性的股票

        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()
        vol_ma20 = vol.rolling(20).mean()
        
        is_stage_2 = bool(current_price > ma50.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1] and ma200.iloc[-1] > ma200.iloc[-20])
        
        rs_line = (close / spy_df).fillna(method='ffill')
        rs_3m = safe_div(current_price, close.iloc[-63])
        rs_score = (rs_3m * 2) + safe_div(current_price, close.iloc[-126]) + safe_div(current_price, close.iloc[-252])
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(252).max())
        
        up_vol = vol[close > close.shift(1)].tail(50).sum()
        dn_vol = vol[close < close.shift(1)].tail(50).sum()
        ud_ratio = safe_div(up_vol, dn_vol)

        tightness = safe_div(close.tail(10).std(), close.tail(10).mean()) * 100
        
        daily_range = high.iloc[-1] - low.iloc[-1]
        close_quality = safe_div(current_price - low.iloc[-1], daily_range)
        is_good_close = bool(close_quality > 0.55 or daily_range == 0)

        dist_ma50_pct = safe_div(current_price - ma50.iloc[-1], ma50.iloc[-1])

        # 潜龙早鸣：不追高 + 均线确认
        is_early_bird = bool(
            spy_is_healthy and 
            is_good_close and  
            current_price > ma50.iloc[-1] and 
            dist_ma50_pct < 0.12 and            
            ma20.iloc[-1] > ma50.iloc[-1] * 0.98 and 
            vol.iloc[-1] > vol_ma20.iloc[-1] * 1.6 and 
            rs_3m > 1.05 and 
            ud_ratio > 1.15
        )
        
        action = "观察"
        rs_stealth = bool(rs_nh and current_price < close.tail(20).max() * 1.02)
        v_dry = bool(vol.iloc[-1] < vol.tail(10).mean() * 0.7)
        
        if is_early_bird and not is_stage_2: action = "🐣 潜龙早鸣(防追高)"
        elif rs_stealth and tightness < 1.5: action = "👁️ 奇点先行(RS Stealth)"
        elif is_stage_2 and abs(dist_ma50_pct) < 0.025 and v_dry: action = "🐉 老龙回头(V-Dry)"
        elif rs_nh and current_price >= close.tail(252).max(): action = "🚀 动量爆发(Breakout)"
        elif is_stage_2 and rs_nh: action = "💎 双重共振(Leader)"
        
        adr = ((high - low)/low).tail(20).mean() 
        stop_price = current_price * (1 - adr * 1.8)
        risk_per_share = current_price - stop_price
        shares = math.floor((ACCOUNT_SIZE * 0.01) / risk_per_share) if risk_per_share > 0 else 0

        return {
            "score": rs_score, "action": action, "tight": tightness, "price": current_price,
            "stop": stop_price, "shares": shares, "ud": ud_ratio, "rs_nh": rs_nh,
            "adr": adr * 100, "dollar_vol": dollar_vol, 
            "is_stage_2": is_stage_2, "is_early_bird": is_early_bird
        }
    except: return None

# ==========================================
# 3. 自动化扫描引擎
# ==========================================
def run_v750_apex_sentinel():
    print(f"📡 [1/3] V750 巅峰指挥部：正在探测...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        macro = yf.download(["SPY", "^VIX", "DX-Y.NYB"], period="1y", progress=False)['Close']
        vix_series = macro["^VIX"].dropna()
        vix = float(vix_series.iloc[-1]) if len(vix_series) > 0 else 20.0
        
        spy_macro = macro["SPY"].dropna()
        spy_is_healthy = bool(spy_macro.iloc[-1] > spy_macro.tail(50).mean()) if len(spy_macro) > 50 else True
    except Exception as e:
        print(f"⚠️ 宏观数据获取失败，使用默认值: {e}")
        vix = 20.0
        spy_is_healthy = True

    try:
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        sp_list = sp_df['Symbol'].str.replace('.', '-').tolist()
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(set(sp_list + CORE_LEADERS))
    except:
        tickers = CORE_LEADERS; ticker_sector_map = {t: "Leaders" for t in tickers}

    data = yf.download(list(set(tickers + ["SPY"])), period="2y", group_by='ticker', threads=True, progress=False)
    
    try:
        spy_df = data["SPY"]["Close"].dropna()
    except:
        print("❌ 无法获取 SPY 参照数据，程序终止。")
        return

    valid_ts = [t for t in tickers if t in data.columns.levels[0]] if isinstance(data.columns, pd.MultiIndex) else ["SPY"]
    breadth_c = 0
    valid_count = 0
    for t in valid_ts[:250]:
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 50:
                valid_count += 1
                if float(c.iloc[-1]) > float(c.tail(50).mean()):
                    breadth_c += 1
        except: continue
    breadth = (breadth_c / valid_count * 100) if valid_count > 0 else 50.0

    print(f"🚀 [2/3] 执行审计 (SPY健康: {spy_is_healthy} / VIX: {vix:.2f} / 宽度: {breadth:.1f}%)...")
    candidates = []
    for t in valid_ts:
        try:
            df = data[t].dropna()
            if len(df) < 252: continue
            
            v750 = calculate_v750_apex_engine(df, spy_df, spy_is_healthy)
            if not v750 or v750['action'] == "观察": continue
            
            if vix > 29 and "🚀" in v750['action']: continue
            if not v750['is_stage_2'] and not v750['is_early_bird']: continue

            candidates.append({
                "Ticker": t, "Action": v750['action'], "Score": v750['score'], 
                "Sector": ticker_sector_map.get(t, "Other"), "Price": v750['price'],
                "建议买入": v750['shares'], "止损位": v750['stop'], 
                "U/D比": v750['ud'], "紧致度": v750['tight'], "ADR%": v750['adr'],
                "RS新高": "🌟" if v750['rs_nh'] else "-", "Stock_Dollar_Vol": v750['dollar_vol']
            })
        except: continue

    if not candidates: final_output([], vix, breadth, "平稳"); return
    
    cand_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    final_seeds = cand_df.groupby("Sector").head(2).sort_values(by="Score", ascending=False).head(10)

    print(f"🔥 [3/3] 正在审计期权异动...")
    results = []
    weather = "☀️ 极佳" if (breadth > 60 and vix < 21) else "⛈️ 风险" if (breadth < 40 or vix > 28) else "☁️ 震荡"

    for _, row in final_seeds.iterrows():
        uoa_status, call_pct, opt_vol = get_apex_uoa_intel(row['Ticker'])
        os_ratio = safe_div(opt_vol, row['Stock_Dollar_Vol'])
        try:
            cal = yf.Ticker(row['Ticker']).calendar
            e_str = cal.iloc[0, 0].date().strftime('%m-%d') if (cal is not None and not cal.empty) else "未知"
        except: e_str = "未知"

        rating = "💎SSS" if (call_pct > 64 and "🔥" in uoa_status and "👁️" in row['Action']) else \
                 "🚀SS" if ("🐣" in row['Action'] and call_pct > 60) else "🔥强势"

        row_dict = row.to_dict()
        row_dict.update({"财报日": e_str, "期权看涨%": call_pct, "期权异动": uoa_status, "期现比": f"{round(os_ratio*100, 1)}%", "评级": rating})
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
        
        beijing_tz = datetime.timezone(datetime.timedelta(hours=8))
        bj_time_str = datetime.datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M')

        header = [
            ["🏰 [V750 哨兵巅峰 - 机构纯净版]", "", "Update(北京时间):", bj_time_str],
            ["当前天气:", weather, "宽度(50MA):", f"{round(breadth, 1)}%", "VIX指数:", round(vix, 2)],
            ["大师指令:", "已开启机构护城河：过滤 $10 以下及日均成交额不足千万美元的弱流动性标的。"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "期权异动", "Price", "建议买入", "止损位", "U/D比", "紧致度", "期权看涨%", "期现比", "财报日", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            sh.update(values=[df.columns.tolist()] + [[robust_json_clean(c) for c in r] for r in df.values.tolist()], range_name="A5")
        else:
            sh.update_acell("A5", "📭 全域审计完成：当前环境未探测到符合形态的信号。")
        print(f"🎉 V750 巅峰指令下达！(北京时间: {bj_time_str})")
    except Exception as e: 
        print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    try:
        run_v750_apex_sentinel()
    except Exception as e:
        print(f"🚨 程序遭遇致命错误崩溃: {e}")
        traceback.print_exc()
