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

# 禁用 yfinance 的本地数据库缓存，防止 OperationalError: database is locked
import yfinance.utils as yf_utils
try:
    yf.set_tz_cache_location(None)
except:
    pass

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
POLYGON_API_KEY = "您的_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
creds_file = "credentials.json"

# ==========================================
# 🛡️ 核心：绝对零度清洗 (彻底根治 JSON 兼容问题)
# ==========================================
def absolute_zero_clean(val):
    """
    终极清洗：强制切断所有与 Numpy/Pandas 的联系
    """
    try:
        # 如果是 Numpy 的数字类型
        if isinstance(val, (np.floating, np.integer)):
            val = val.item() # 转换为 Python 原生数字
        
        if isinstance(val, float):
            if math.isnan(val) or math.isinf(val):
                return 0.0
            return float(round(val, 3))
        
        if isinstance(val, int):
            return int(val)
        
        if val is None:
            return ""
            
        return str(val)
    except:
        return str(val)

def safe_div(n, d):
    try:
        n_val = float(n)
        d_val = float(d)
        if d_val == 0 or not math.isfinite(d_val) or not math.isfinite(n_val):
            return 0.0
        return n_val / d_val
    except:
        return 0.0

# ==========================================
# 2. V55 核心演算逻辑
# ==========================================
def calculate_v55_metrics(df, spy_df):
    try:
        close = df['Close']
        vol = df['Volume']
        
        # 1. 垂直加速度 (感知 CF/PR 的点火)
        rs_line = (close / spy_df['Close']).fillna(method='ffill')
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(20).max())
        
        slope_now = safe_div(rs_line.iloc[-1] - rs_line.iloc[-6], rs_line.iloc[-6])
        slope_prev = safe_div(rs_line.iloc[-7] - rs_line.iloc[-12], rs_line.iloc[-12])
        acceleration = slope_now - slope_prev
        
        # 2. 紧致度 (感知 GOOGL 的蓄势)
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean())
        
        # 3. U/D 量能比
        up_v = vol[df['Close'] > df['Open']].tail(40).sum()
        dn_v = vol[df['Close'] < df['Open']].tail(40).sum()
        ud_ratio = safe_div(up_v, dn_v)
        
        # 4. ATR 动态止盈
        tr = pd.concat([(df['High']-df['Low']), (df['High']-close.shift(1)).abs(), (df['Low']-close.shift(1)).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        atr_val = float(atr) if math.isfinite(atr) else float(close.iloc[-1] * 0.03)
        trailing_stop = float(close.iloc[-1]) - (2.5 * atr_val)
        
        # 5. 筹码中心 POC
        counts, bin_edges = np.histogram(close.tail(120).values, bins=50, weights=vol.tail(120).values)
        poc = (bin_edges[np.argmax(counts)] + bin_edges[np.argmax(counts)+1]) / 2
        
        # 6. 综合评分
        stock_ret = safe_div(close.iloc[-1], close.iloc[-63])
        spy_ret = safe_div(spy_df['Close'].iloc[-1], spy_df['Close'].iloc[-63])
        rs_raw = safe_div(stock_ret, spy_ret)
        score = rs_raw * ud_ratio * safe_div(1, (tightness * 100))
        
        return {
            "score": float(score), "acceleration": float(acceleration), "rs_nh": rs_nh,
            "tightness": float(tightness*100), "ud_ratio": float(ud_ratio), 
            "trailing_stop": float(trailing_stop), "poc": float(poc), "rs_raw": float(rs_raw),
            "dist_high": safe_div(close.iloc[-1] - df['High'].max(), df['High'].max())
        }
    except:
        return None

# ==========================================
# 3. 执行引擎
# ==========================================
def run_v55_citadel():
    print("📡 [1/3] 天基指挥部启动：正在执行全球同步扫描...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        sp_tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)
        raw_list = sp_tables[0]['Symbol'].tolist()
        tickers = list(set([t.replace('.', '-') for t in raw_list] + ["PR", "CF", "NTR", "NVDA", "GOOGL"]))
    except Exception as e:
        print(f"❌ 无法获取名册: {e}")
        return

    # 关键：SPY 和宏观数据禁用多线程，确保下载成功
    env = yf.download(["DX-Y.NYB", "^VIX", "SPY"], period="5d", progress=False, threads=False)['Close']
    if "SPY" not in env.columns or env["SPY"].isnull().all():
        print("❌ SPY 数据下载失败，无法计算相对强度，系统熔断。")
        return
        
    vix = float(env["^VIX"].iloc[-1]) if "^VIX" in env.columns and not env["^VIX"].isnull().all() else 20.0
    
    # 大批量个股下载
    data = yf.download(tickers + ["SPY"], period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"].dropna()

    pre_candidates = []
    ma50_up_count = 0
    total_valid = 0

    print(f"🚀 [2/3] 执行‘天基感知’演算 (共 {len(tickers)} 只)...")
    
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 150: continue
            
            total_valid += 1
            close = float(df['Close'].iloc[-1])
            ma50_val = float(df['Close'].tail(50).mean())
            if close > ma50_val: ma50_up_count += 1
            if close < float(df['Close'].tail(200).mean()): continue 
            
            v55 = calculate_v55_metrics(df, spy_df)
            if not v55: continue
            
            # 感知判定
            is_explosion = (v55['dist_high'] >= -0.05) and (v55['acceleration'] > 0)
            is_stealth = v55['rs_nh'] and (v55['tightness'] < 1.0)
            is_dip = (0 <= (close - v55['poc'])/v55['poc'] <= 0.06) and (v55['rs_raw'] > 1.1)
            
            if is_explosion or is_stealth or is_dip:
                action = "🚀垂直爆破" if is_explosion else "👁️暗盘先行" if is_stealth else "🐉支撑回踩"
                pre_candidates.append({
                    "Ticker": t, "Action": action, "总分": v55['score'], 
                    "RS线": "🌟先行" if v55['rs_nh'] else "-", "加速": "仰攻📈" if v55['acceleration'] > 0 else "平稳",
                    "移动止盈": v55['trailing_stop'], "POC支撑": v55['poc'], "Price": close, "紧致度": v55['tightness']
                })
        except: continue

    breadth = ma50_up_count / total_valid if total_valid > 0 else 0
    weather = "☀️ 极佳" if (breadth > 0.6 and vix < 22) else "⛈️ 风险" if (breadth < 0.4 or vix > 28) else "☁️ 震荡"

    seeds = sorted(pre_candidates, key=lambda x: x['总分'], reverse=True)[:5]

    print(f"🔥 [3/3] 期权哨兵核验中...")
    results = []
    for item in seeds:
        opt_score, opt_desc = get_sentiment_v55(item['Ticker'])
        try:
            t_obj = yf.Ticker(item['Ticker'])
            cal = t_obj.calendar
            days_to_e = (cal.iloc[0, 0].date() - datetime.date.today()).days if cal is not None and not cal.empty else 99
            eb_str = "⚠️临近" if 0 <= days_to_e <= 7 else "安全"
        except: eb_str = "未知"
        
        item.update({
            "评级": "💎SSS+" if (opt_score > 65 and weather == "☀️ 极佳") else "🔥强势",
            "财报": eb_str, "期权看涨%": opt_score, "期权规模": opt_desc
        })
        results.append(item)
        time.sleep(13)

    final_output(results, weather, breadth, vix)

def get_sentiment_v55(ticker):
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        bull, total = 0, 0
        for s in snaps:
            val = s.day.volume * (s.day.last or 0) * 100
            if val > 50000:
                total += val
                if s.details.contract_type == 'call': bull += val
        return round(safe_div(bull, total)*100, 1) if total > 0 else 50.0, f"${round(total/1e6, 2)}M"
    except: return 50.0, "N/A"

def final_output(res, weather, breadth, vix):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        header = [
            ["🏰 [V55 天基指挥部 - 绝对零度版]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["环境天气:", weather, "大盘宽度:", f"{round(breadth*100, 1)}%", "VIX:", round(vix, 2)],
            ["战术逻辑:", "【👁️暗盘先行】感知权重股调仓，【🚀垂直爆破】感知CF/PR起爆。"],
            ["", "", "", ""]
        ]
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "RS线", "加速", "移动止盈", "POC支撑", "财报", "Price", "期权看涨%", "期权规模", "紧致度"]
            df = df[cols]
            
            # --- 绝对零度环节：强制类型脱壳 ---
            raw_matrix = [df.columns.tolist()] + df.values.tolist()
            final_clean_matrix = [[absolute_zero_clean(cell) for cell in row] for row in raw_matrix]
            
            sh.update(values=header, range_name="A1")
            sh.update(values=final_clean_matrix, range_name="A5")
        else:
            sh.update(values=header, range_name="A1")
            sh.update_acell("A5", "今日无符合天基信号的目标。")
            
        print("🎉 V55 任务圆满完成！数据已强制净化。")
    except Exception as e:
        print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    run_v55_citadel()
