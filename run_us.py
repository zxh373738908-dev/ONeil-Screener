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

CORE_LEADERS =["NVDA", "GOOGL", "CF", "PR", "TSLA", "PLTR", "META", "AVGO", "COST", "AAPL", "MSFT", "AMZN"]

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
# 2. V750 巅峰引擎 7.0 (逼空绞肉机 + VWAP成本线)
# ==========================================
def calculate_v750_apex_engine(df, spy_df, spy_is_healthy):
    try:
        close, high, low, vol = df['Close'], df['High'], df['Low'], df['Volume']
        
        current_price = close.iloc[-1]
        dollar_vol = (vol.tail(5) * close.tail(5)).mean()
        
        if current_price < 10.0 or dollar_vol < 10_000_000:
            return None

        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()
        ema10 = close.ewm(span=10, adjust=False).mean() 
        vol_ma20 = vol.rolling(20).mean()
        
        # 🚀 新增：计算近 3 日短线资金真实成交重心 (Anchored VWAP 近似)
        typical_price = (high.tail(3) + low.tail(3) + close.tail(3)) / 3
        volume_tail = vol.tail(3)
        short_term_vwap = safe_div((typical_price * volume_tail).sum(), volume_tail.sum())
        if short_term_vwap == 0: short_term_vwap = current_price
        
        is_stage_2 = bool(current_price > ma50.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1] and ma200.iloc[-1] > ma200.iloc[-20])
        
        rs_line = (close / spy_df).ffill()
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

        dist_ema10_pct = safe_div(current_price - ema10.iloc[-1], ema10.iloc[-1])

        is_early_bird = bool(
            spy_is_healthy and 
            is_good_close and  
            current_price > ema10.iloc[-1] and 
            abs(dist_ema10_pct) < 0.08 and            
            ma20.iloc[-1] > ma50.iloc[-1] * 0.98 and 
            vol.iloc[-1] > vol_ma20.iloc[-1] * 1.5 and 
            rs_3m > 1.05 and 
            ud_ratio > 1.05  
        )
        
        # ⚔️ 升级版早盘诱空反包：必须强力收复近3日主力 VWAP 成本线
        is_morning_trap = bool(
            is_good_close and 
            low.iloc[-1] < ema10.iloc[-1] and 
            current_price > ema10.iloc[-1] and
            current_price > short_term_vwap and  # 🔥 引入真实日内博弈护城河
            vol.iloc[-1] > vol_ma20.iloc[-1] * 0.8
        )
        
        action = "观察"
        rs_stealth = bool(rs_nh and current_price < close.tail(20).max() * 1.02)
        
        if is_morning_trap: action = "⚔️ 早盘诱空反包"
        elif is_early_bird and not is_stage_2: action = "🐣 潜龙早鸣(防追高)"
        elif rs_stealth and tightness < 1.5: action = "👁️ 奇点先行(RS Stealth)"
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
            "is_stage_2": is_stage_2, "is_early_bird": is_early_bird, "is_morning_trap": is_morning_trap,
            "dist_ema10_pct": dist_ema10_pct
        }
    except: return None

# ==========================================
# 3. 自动化扫描引擎 (深度优化提速版)
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
        vix = 20.0
        spy_is_healthy = True

    try:
        sp_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        sp_list = sp_df['Symbol'].str.replace('.', '-').tolist()
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(set(sp_list + CORE_LEADERS))
    except:
        tickers = CORE_LEADERS; ticker_sector_map = {t: "Leaders" for t in tickers}

    data = yf.download(list(set(tickers +["SPY"])), period="2y", group_by='ticker', threads=True, progress=False)
    
    try:
        spy_df = data["SPY"]["Close"].dropna()
    except:
        return

    valid_ts = [t for t in tickers if t in data.columns.levels[0]] if isinstance(data.columns, pd.MultiIndex) else ["SPY"]
    breadth_c, valid_count = 0, 0
    for t in valid_ts[:250]:
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 50:
                valid_count += 1
                if float(c.iloc[-1]) > float(c.tail(50).mean()): breadth_c += 1
        except: continue
    breadth = (breadth_c / valid_count * 100) if valid_count > 0 else 50.0

    print(f"🚀 [2/3] 执行审计 (SPY健康: {spy_is_healthy} / VIX: {vix:.2f} / 宽度: {breadth:.1f}%)...")
    candidates =[]
    for t in valid_ts:
        try:
            df = data[t].dropna()
            if len(df) < 252: continue
            
            v750 = calculate_v750_apex_engine(df, spy_df, spy_is_healthy)
            if not v750 or v750['action'] == "观察": continue
            if vix > 29 and "🚀" in v750['action']: continue
            if not v750['is_stage_2'] and not v750['is_early_bird'] and not v750['is_morning_trap']: continue

            candidates.append({
                "Ticker": t, 
                "Action": v750['action'], 
                "Score": round(v750['score'], 2), 
                "Sector": ticker_sector_map.get(t, "Other"), 
                "Price": f"${v750['price']:.2f}",               
                "建议买入": f"{v750['shares']} 股",  
                "止损位": f"${v750['stop']:.2f}",                 
                "U/D比": f"{v750['ud']:.2f}",       
                "紧致度": f"{v750['tight']:.2f}%",    
                "ADR%": v750['adr'],
                "RS新高": "🌟" if v750['rs_nh'] else "-", 
                "Stock_Dollar_Vol": v750['dollar_vol'],
                "Dist_EMA10%": f"{v750['dist_ema10_pct']*100:.2f}%"
            })
        except: continue

    if not candidates: final_output([], vix, breadth, "平稳"); return
    
    cand_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    
    # 🚀 突破限制：为了照顾 Polygon 免费版 API (每分钟5次)，只提取全市场战力评分最强的前 5 只怪物进行期权审计！
    final_seeds = cand_df.head(5) 

    print(f"🔥 [3/3] 正在审计期权异动及【空头燃料】(锁定最强 TOP 5)...")
    results =[]
    weather = "☀️ 极佳" if (breadth > 60 and vix < 21) else "⛈️ 风险" if (breadth < 40 or vix > 28) else "☁️ 震荡"

    for _, row in final_seeds.iterrows():
        ticker = row['Ticker']
        uoa_status, call_pct, opt_vol = get_apex_uoa_intel(ticker)
        os_ratio = safe_div(opt_vol, row['Stock_Dollar_Vol'])
        
        e_str = "未知"
        try:
            t_info = yf.Ticker(ticker)
            cal = t_info.calendar
            if isinstance(cal, dict) and 'Earnings Date' in cal:
                dates = cal['Earnings Date']
                if dates and len(dates) > 0: e_str = dates[0].strftime('%m-%d')
            elif hasattr(cal, 'iloc') and not cal.empty:
                e_str = cal.iloc[0, 0].date().strftime('%m-%d') if hasattr(cal.iloc[0, 0], 'date') else cal.iloc[0, 0].strftime('%m-%d')
        except: pass
            
        # 🚀 稳健升级：完美防御 YFinance 爬虫被封锁时的 NoneType 报错
        try:
            info = t_info.info
            short_pct = float(info.get('shortPercentOfFloat') or 0.0)
            short_ratio = float(info.get('shortRatio') or 0.0)
        except Exception as e: 
            short_pct, short_ratio = 0.0, 0.0

        is_sqz = (short_pct > 0.05) and (short_ratio > 3)
        sqz_label = f"{short_pct*100:.2f}%({short_ratio:.1f}D)"
        if is_sqz: sqz_label += " 【🔥空头核爆区】"

        rating = "💎SSS" if (call_pct > 64 and "🔥" in uoa_status and "👁️" in row['Action']) else \
                 "🚀SS" if (("⚔️" in row['Action'] or "🐣" in row['Action']) and call_pct > 60) else "🔥强势"

        row_dict = row.to_dict()
        row_dict.update({
            "财报日": e_str, 
            "期权看涨%": f"{call_pct:.1f}%", 
            "期权异动": uoa_status, 
            "期现比": f"{round(os_ratio*100, 1)}%", 
            "评级": rating,
            "Short_SqZ": sqz_label
        })
        results.append(row_dict)
        time.sleep(12.5) # 必须保留，这是向 Polygon 免费白嫖数据的过路费 

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
        sh.format("A1:Z100", {
            "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}, 
            "textFormat": {"foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}, "bold": False},
            "numberFormat": {"type": "AUTOMATIC"}  
        })
        
        beijing_tz = datetime.timezone(datetime.timedelta(hours=8))
        bj_time_str = datetime.datetime.now(beijing_tz).strftime('%Y-%m-%d %H:%M')

        header = [
            ["🏰[V750 哨兵巅峰 7.0 - 终极逼空绞肉机]", "", "Update(北京时间):", bj_time_str],["当前天气:", weather, "宽度(50MA):", f"{round(breadth, 1)}%", "VIX指数:", round(vix, 2)],["大师指令:", "已开启 7.0 极速防崩引擎：抓取 TOP5 猎物。融合 VWAP 主力成本线辅助反包判定。API并发批量涂色生效中。"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        sh.format("A1:A3", {"textFormat": {"bold": True}})
        
        if res:
            df = pd.DataFrame(res)
            cols =["Ticker", "评级", "Action", "Short_SqZ", "Dist_EMA10%", "期权异动", "Price", "建议买入", "止损位", "U/D比", "紧致度", "期权看涨%", "期现比", "财报日", "Sector"]
            df = df[[c for c in cols if c in df.columns]]
            sh.update(values=[df.columns.tolist()] + [[robust_json_clean(c) for c in r] for r in df.values.tolist()], range_name="A5")
            
            sh.format("A5:O5", {"backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}, "textFormat": {"bold": True}})
            
            # 🚀 降维打击：Google Sheets API 批量涂色，1次请求搞定，彻底粉碎 429 崩溃限制
            red_rows =[i + 6 for i, r in enumerate(res) if "【🔥空头核爆区】" in r.get("Short_SqZ", "")]
            if red_rows:
                formats_payload =
