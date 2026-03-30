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
# 🛡️ 核心工具：数据脱壳净化 (解决 JSON 报错)
# ==========================================
def robust_json_clean(val):
    """
    终极净化：确保所有数据类型均为 Python 原生，且无 NaN/Inf
    """
    try:
        # 处理 Numpy 数值对象
        if hasattr(val, 'item'):
            val = val.item()
            
        if isinstance(val, (float, int)):
            if not math.isfinite(val):
                return 0.0
            return float(round(val, 3)) if isinstance(val, float) else int(val)
        
        if val is None:
            return ""
        
        return str(val)
    except:
        return str(val)

def safe_div(n, d):
    try:
        n_f, d_f = float(n), float(d)
        if d_f == 0 or not math.isfinite(d_f) or not math.isfinite(n_f):
            return 0.0
        return n_f / d_f
    except:
        return 0.0

# ==========================================
# 2. V60 核心算法库 (感知 GOOGL/CF/PR)
# ==========================================
def calculate_v60_metrics(df, spy_df):
    try:
        close = df['Close']
        vol = df['Volume']
        
        # 1. 垂直加速度 (感知 CF/PR 的点火瞬间)
        rs_line = (close / spy_df['Close']).fillna(method='ffill')
        # RS线创20日新高判定 (暗盘先行信号)
        rs_nh = bool(rs_line.iloc[-1] >= rs_line.tail(20).max())
        
        # 二阶导：加速度
        slope_now = safe_div(rs_line.iloc[-1] - rs_line.iloc[-6], rs_line.iloc[-6])
        slope_prev = safe_div(rs_line.iloc[-7] - rs_line.iloc[-12], rs_line.iloc[-12])
        acceleration = slope_now - slope_prev
        
        # 2. 紧致度 (感知 GOOGL 起爆前的收缩)
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean())
        
        # 3. 量能 U/D 比
        up_v = vol[df['Close'] > df['Open']].tail(40).sum()
        dn_v = vol[df['Close'] < df['Open']].tail(40).sum()
        ud_ratio = safe_div(up_v, dn_v)
        
        # 4. ATR 动态移动止盈
        tr = pd.concat([(df['High']-df['Low']), (df['High']-close.shift(1)).abs(), (df['Low']-close.shift(1)).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        atr_val = float(atr) if math.isfinite(atr) else float(close.iloc[-1] * 0.03)
        trailing_stop = float(close.iloc[-1]) - (2.5 * atr_val)
        
        # 5. 筹码中心 POC
        counts, bin_edges = np.histogram(close.tail(120).values, bins=50, weights=vol.tail(120).values)
        poc = (bin_edges[np.argmax(counts)] + bin_edges[np.argmax(counts)+1]) / 2
        
        # 6. 综合评分 (奇点得分)
        rs_raw = safe_div(close.iloc[-1]/close.iloc[-63], spy_df['Close'].iloc[-1]/spy_df['Close'].iloc[-63])
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
# 3. 选股扫描引擎
# ==========================================
def run_v60_citadel():
    print("📡 [1/3] 天基指挥部：正在同步核心指数与宏观因子...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 强制下载关键指数 (不使用多线程，防止锁定)
    idx_data = yf.download(["SPY", "^VIX", "DX-Y.NYB"], period="10d", threads=False, progress=False)['Close']
    if "SPY" not in idx_data.columns or idx_data["SPY"].dropna().empty:
        print("❌ 核心数据 SPY 缺失，系统熔断。")
        return
    
    spy_latest = idx_data["SPY"].dropna()
    vix_val = float(idx_data["^VIX"].iloc[-1]) if "^VIX" in idx_data.columns else 20.0
    
    # 获取全名册
    try:
        sp_tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)
        raw_list = sp_tables[0]['Symbol'].tolist()
        tickers = list(set([t.replace('.', '-') for t in raw_list] + ["PR", "CF", "NTR", "NVDA", "GOOGL"]))
    except:
        print("❌ 无法获取 Ticker 名册。")
        return

    # 批量下载个股
    print(f"🚀 [2/3] 执行‘奇点感知’演算 (共 {len(tickers)} 只)...")
    data = yf.download(tickers + ["SPY"], period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"].dropna()

    pre_candidates = []
    ma50_up_count = 0
    total_valid = 0

    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 150: continue
            
            total_valid += 1
            close = float(df['Close'].iloc[-1])
            ma50_v = float(df['Close'].tail(50).mean())
            if close > ma50_v: ma50_up_count += 1
            
            # 二阶段趋势过滤
            if close < float(df['Close'].tail(200).mean()): continue 
            
            v60 = calculate_v60_metrics(df, spy_df)
            if not v60: continue
            
            # --- 提早感知逻辑 ---
            # 模式 A: 垂直爆破 (捕捉 CF/PR)
            is_explosion = (v60['dist_high'] >= -0.05) and (v60['acceleration'] > 0)
            # 模式 B: 暗盘先行 (捕捉 GOOGL)
            is_stealth = v60['rs_nh'] and (v60['tightness'] < 1.0)
            # 模式 C: 经典回踩
            is_dip = (0 <= (close - v60['poc'])/v60['poc'] <= 0.06) and (v60['rs_raw'] > 1.1)
            
            if is_explosion or is_stealth or is_dip:
                action = "🚀垂直爆破" if is_explosion else "👁️暗盘先行" if is_stealth else "🐉支撑回踩"
                pre_candidates.append({
                    "Ticker": t, "Action": action, "总分": v60['score'], 
                    "RS线": "🌟先行" if v60['rs_nh'] else "-", "加速": "仰攻📈" if v60['acceleration'] > 0 else "平稳",
                    "移动止盈": v60['trailing_stop'], "POC支撑": v60['poc'], "Price": close, "紧致度": v60['tightness']
                })
        except: continue

    # 天气系统
    breadth = ma50_up_count / total_valid if total_valid > 0 else 0
    weather = "☀️ 极佳" if (breadth > 0.6 and vix_val < 22) else "⛈️ 风险" if (breadth < 0.4 or vix_val > 28) else "☁️ 震荡"

    # 排序前 5
    seeds = sorted(pre_candidates, key=lambda x: x['总分'], reverse=True)[:5]

    print(f"🔥 [3/3] 期权哨兵终审中...")
    results = []
    for item in seeds:
        opt_score, opt_desc = get_sentiment(item['Ticker'])
        try:
            t_obj = yf.Ticker(item['Ticker'])
            cal = t_obj.calendar
            eb_str = "⚠️临近" if (cal is not None and not cal.empty and (cal.iloc[0, 0].date() - datetime.date.today()).days <= 7) else "安全"
        except: eb_str = "未知"
        
        item.update({
            "评级": "💎SSS+" if (opt_score > 65 and weather == "☀️ 极佳") else "🔥强势",
            "财报": eb_str, "期权看涨%": opt_score, "期权规模": opt_desc
        })
        results.append(item)
        time.sleep(13)

    final_output(results, weather, breadth, vix_val)

def get_sentiment(ticker):
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
            ["🏰 [V60 天基指挥部 - 不坏金身版]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["环境天气:", weather, "大盘宽度:", f"{round(breadth*100, 1)}%", "VIX:", round(vix, 2)],
            ["操作指令:", "关注【👁️暗盘先行】感知GOOGL调仓，关注【🚀垂直爆破】感知CF/PR起爆。"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "RS线", "加速", "移动止盈", "POC支撑", "财报", "Price", "期权看涨%", "期权规模", "紧致度"]
            df = df[cols]
            
            # --- 终极环节：脱离 Numpy 依赖的列表解析 ---
            raw_data = [df.columns.tolist()] + df.values.tolist()
            final_clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            
            sh.update(values=final_clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "今日无符合天基信号。")
            
        print(f"🎉 V60 指令已下达！大盘状态：{weather}")
    except Exception as e:
        print(f"❌ 最终写入失败: {e}")

if __name__ == "__main__":
    run_v60_citadel()
