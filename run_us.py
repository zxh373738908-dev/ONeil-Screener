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
# 🛡️ 核心：核级数据净化器 (彻底解决 JSON 报错)
# ==========================================
def json_shield(val):
    """
    终极防护：强制将所有数据转换为 JSON 绝对兼容的格式
    """
    # 处理 Numpy 的各种数值类型
    if isinstance(val, (float, np.float64, np.float32, np.float16)):
        if math.isnan(val) or math.isinf(val):
            return 0.0
        return float(round(val, 3))
    if isinstance(val, (int, np.int64, np.int32)):
        return int(val)
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return ""
    # 其他类型全部转为字符串，确保安全
    return str(val)

def safe_div(n, d):
    """防爆除法"""
    try:
        if d == 0 or not math.isfinite(d): return 0.0
        res = n / d
        return float(res) if math.isfinite(res) else 0.0
    except:
        return 0.0

# ==========================================
# 2. V50 核心演算逻辑
# ==========================================
def calculate_v50_metrics(df, spy_df):
    try:
        close = df['Close']
        vol = df['Volume']
        
        # 1. 垂直加速度 (感知 CF/PR 的点火瞬间)
        rs_line = (close / spy_df['Close']).fillna(method='ffill')
        rs_nh = rs_line.iloc[-1] >= rs_line.tail(20).max() # RS线20日新高
        
        slope_now = safe_div(rs_line.iloc[-1] - rs_line.iloc[-6], rs_line.iloc[-6])
        slope_prev = safe_div(rs_line.iloc[-7] - rs_line.iloc[-12], rs_line.iloc[-12])
        acceleration = slope_now - slope_prev # 加速度
        
        # 2. 紧致度 (感知 GOOGL 的蓄势阶段)
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean())
        
        # 3. U/D 量能比
        up_v = vol[df['Close'] > df['Open']].tail(40).sum()
        dn_v = vol[df['Close'] < df['Open']].tail(40).sum()
        ud_ratio = safe_div(up_v, dn_v)
        
        # 4. ATR 动态移动止盈
        tr = pd.concat([(df['High']-df['Low']), (df['High']-close.shift(1)).abs(), (df['Low']-close.shift(1)).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        atr_val = float(atr) if math.isfinite(atr) else float(close.iloc[-1] * 0.03)
        trailing_stop = float(close.iloc[-1]) - (2.5 * atr_val)
        
        # 5. 筹码中心 POC
        counts, bin_edges = np.histogram(close.tail(120), bins=50, weights=vol.tail(120))
        poc = (bin_edges[np.argmax(counts)] + bin_edges[np.argmax(counts)+1]) / 2
        
        # 6. 综合评分 (奇点分)
        stock_ret = safe_div(close.iloc[-1], close.iloc[-63])
        spy_ret = safe_div(spy_df['Close'].iloc[-1], spy_df['Close'].iloc[-63])
        rs_raw = safe_div(stock_ret, spy_ret)
        score = rs_raw * ud_ratio * safe_div(1, (tightness * 100))
        
        return {
            "score": score, "acceleration": acceleration, "rs_nh": rs_nh,
            "tightness": tightness*100, "ud_ratio": ud_ratio, 
            "trailing_stop": trailing_stop, "poc": poc, "rs_raw": rs_raw,
            "dist_high": safe_div(close.iloc[-1] - df['High'].max(), df['High'].max())
        }
    except:
        return None

# ==========================================
# 3. 执行引擎
# ==========================================
def run_v50_citadel():
    print("📡 [1/3] 天基指挥部启动：正在执行宏观共振与多维感知分析...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        sp_tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)
        raw_list = sp_tables[0]['Symbol'].tolist()
        tickers = list(set([t.replace('.', '-') for t in raw_list] + ["PR", "CF", "NTR", "NVDA", "GOOGL"]))
    except Exception as e:
        print(f"❌ 无法连接名册: {e}")
        return

    # 环境监控
    env = yf.download(["DX-Y.NYB", "^VIX", "SPY"], period="5d", progress=False)['Close']
    vix = float(env["^VIX"].iloc[-1]) if not env.empty and "^VIX" in env.columns else 20.0
    
    data = yf.download(tickers + ["SPY"], period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"].dropna()

    pre_candidates = []
    ma50_up_count = 0
    total_valid = 0

    print(f"🚀 [2/3] 正在对 {len(tickers)} 个目标执行‘奇点感知’演算...")
    
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 150: continue
            
            total_valid += 1
            close = float(df['Close'].iloc[-1])
            if close > float(df['Close'].tail(50).mean()): ma50_up_count += 1
            if close < float(df['Close'].tail(200).mean()): continue # 排除空头形态
            
            v50 = calculate_v50_metrics(df, spy_df)
            if not v50: continue
            
            # --- 提早感知判定 ---
            # 模式 A: 垂直爆破 (针对 CF/PR)
            is_explosion = (v50['dist_high'] >= -0.05) and (v50['acceleration'] > 0)
            # 模式 B: 暗盘潜伏 (针对 GOOGL)
            is_stealth = v50['rs_nh'] and (v50['tightness'] < 1.0)
            # 模式 C: 龙回头
            is_dip = (0 <= (close - v50['poc'])/v50['poc'] <= 0.06) and (v50['rs_raw'] > 1.1)
            
            if is_explosion or is_stealth or is_dip:
                action = "🚀垂直爆破" if is_explosion else "👁️暗盘先行" if is_stealth else "🐉支撑回踩"
                pre_candidates.append({
                    "Ticker": t, "Action": action, "总分": v50['score'], 
                    "RS线": "🌟先行" if v50['rs_nh'] else "-", "加速": "仰攻📈" if v50['acceleration'] > 0 else "平稳",
                    "移动止盈": v50['trailing_stop'], "POC支撑": v50['poc'], "Price": close, "紧致度": v50['tightness']
                })
        except: continue

    # 大盘天气
    breadth = ma50_up_count / total_valid if total_valid > 0 else 0
    weather = "☀️ 极佳" if (breadth > 0.6 and vix < 22) else "⛈️ 风险" if (breadth < 0.4 or vix > 28) else "☁️ 震荡"

    # 排序筛选前 5 只进行昂贵的期权扫描
    seeds = sorted(pre_candidates, key=lambda x: x['总分'], reverse=True)[:5]

    print(f"🔥 [3/3] 期权哨兵核验中 (Polygon 限速)...")
    results = []
    for item in seeds:
        opt_score, opt_desc = get_sentiment_v50(item['Ticker'])
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

    final_output_to_sheets(results, weather, breadth, vix)

def get_sentiment_v50(ticker):
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

def final_output_to_sheets(res, weather, breadth, vix):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        # 1. 顶部状态栏
        header_area = [
            ["🏰 [V50 天基指挥部 - 终极无损版]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["环境天气:", weather, "大盘宽度:", f"{round(breadth*100, 1)}%", "VIX:", round(vix, 2)],
            ["战术逻辑:", "【👁️暗盘先行】感知权重股调仓，【🚀垂直爆破】感知CF/PR起爆。"],
            ["", "", "", ""]
        ]
        sh.update(values=header_area, range_name="A1")
        
        # 2. 数据体渲染
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "RS线", "加速", "移动止盈", "POC支撑", "财报", "Price", "期权看涨%", "期权规模", "紧致度"]
            df = df[cols]
            
            # --- 终极环节：强制数据穿梭 ---
            # 我们不直接发送 df，而是通过一层 json_shield 转换成纯 Python List
            raw_matrix = [df.columns.tolist()] + df.values.tolist()
            final_clean_matrix = [[json_shield(cell) for cell in row] for row in raw_matrix]
            
            sh.update(values=final_clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "今日无符合天基信号的目标。")
            
        print("🎉 V50 指令已下达！数据净化通过，Google Sheets 已同步。")
    except Exception as e:
        print(f"❌ 最终写入失败: {e}")

if __name__ == "__main__":
    run_v50_citadel()
