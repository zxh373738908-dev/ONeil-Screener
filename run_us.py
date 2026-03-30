import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import time
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
# 🛡️ 核心工具：数据净化引擎 (彻底根治 JSON 报错)
# ==========================================
def safe_div(n, d):
    """极致安全的除法"""
    try:
        if d == 0 or np.isnan(d) or np.isinf(d): return 0.0
        res = n / d
        return float(res) if np.isfinite(res) else 0.0
    except:
        return 0.0

def purify_for_google_sheets(df):
    """
    工业级清洗：强制将所有 Numpy/Pandas 异常数值转换为标准 Python 类型或 None
    Google Sheets API 将 None 识别为单元格清空，这比 0 更安全
    """
    # 1. 替换所有的 inf 为 nan
    df = df.replace([np.inf, -np.inf], np.nan)
    
    # 2. 核心清洗：使用 where 结合 pd.notnull
    # 将 nan 替换为 None (JSON 兼容的 null)，并强转为 native python 类型
    df = df.where(pd.notnull(df), None)
    
    # 3. 将所有浮点数保留 3 位小数并强转为原生 float
    for col in df.columns:
        if df[col].dtype == 'float64' or df[col].dtype == 'float32':
            df[col] = df[col].apply(lambda x: round(float(x), 3) if x is not None else None)
            
    # 4. 转换回列表
    clean_list = [df.columns.tolist()] + df.values.tolist()
    return clean_list

# ==========================================
# 2. V50 核心算法库
# ==========================================
def calculate_v50_metrics(df, spy_df):
    try:
        close = df['Close']
        vol = df['Volume']
        
        # 1. 垂直加速度与 RS 线先行突破 (提早感知 GOOGL/CF)
        rs_line = (close / spy_df['Close']).fillna(method='ffill')
        rs_nh = rs_line.iloc[-1] >= rs_line.tail(20).max() # RS线创20日新高
        
        slope_now = safe_div(rs_line.iloc[-1] - rs_line.iloc[-6], rs_line.iloc[-6])
        slope_prev = safe_div(rs_line.iloc[-7] - rs_line.iloc[-12], rs_line.iloc[-12])
        acceleration = slope_now - slope_prev # 二阶导：加速度
        
        # 2. 紧致度与 U/D 强度
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean())
        up_v = vol[df['Close'] > df['Open']].tail(40).sum()
        dn_v = vol[df['Close'] < df['Open']].tail(40).sum()
        ud_ratio = safe_div(up_v, dn_v)
        
        # 3. ATR 移动止盈
        tr = pd.concat([(df['High']-df['Low']), (df['High']-close.shift(1)).abs(), (df['Low']-close.shift(1)).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        atr_val = float(atr) if np.isfinite(atr) else float(close.iloc[-1] * 0.03)
        trailing_stop = float(close.iloc[-1]) - (2.5 * atr_val)
        
        # 4. 筹码中心 POC
        counts, bin_edges = np.histogram(close.tail(120), bins=50, weights=vol.tail(120))
        poc = (bin_edges[np.argmax(counts)] + bin_edges[np.argmax(counts)+1]) / 2
        
        # 5. 综合总分 (奇点分)
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
# 3. 指挥部核心扫描
# ==========================================
def run_v50_citadel():
    print("📡 [1/3] 天基指挥部启动：执行全球共振与感知分析...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        sp_tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)
        raw_list = sp_tables[0]['Symbol'].tolist()
        tickers = list(set([t.replace('.', '-') for t in raw_list] + ["PR", "CF", "NTR", "NVDA", "GOOGL"]))
    except Exception as e:
        print(f"❌ 获取名册失败: {e}")
        return

    # 环境监控
    env = yf.download(["DX-Y.NYB", "^VIX", "SPY"], period="5d", progress=False)['Close']
    vix = float(env["^VIX"].iloc[-1]) if not env.empty and "^VIX" in env.columns else 20.0
    
    data = yf.download(tickers + ["SPY"], period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"].dropna()

    pre_candidates = []
    ma50_up_count = 0
    total_valid = 0

    print(f"🚀 [2/3] 正在对 {len(tickers)} 个目标执行‘垂直加速度’演算...")
    
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
            
            v50 = calculate_v50_metrics(df, spy_df)
            if not v50: continue
            
            # 提早感知判定
            # 模式 A: 垂直爆破 (加速+新高附近)
            is_explosion = (v50['dist_high'] >= -0.05) and (v50['acceleration'] > 0)
            # 模式 B: 暗盘突破 (RS线先行 + 紧致度高 - 感知 GOOGL)
            is_stealth = v50['rs_nh'] and (v50['tightness'] < 1.0)
            # 模式 C: 龙回头
            is_dip = (0 <= (close - v50['poc'])/v50['poc'] <= 0.06) and (v50['rs_raw'] > 1.1)
            
            if is_explosion or is_stealth or is_dip:
                action = "🚀垂直爆破" if is_explosion else "👁️暗盘先行" if is_stealth else "🐉支撑回踩"
                pre_candidates.append({
                    "Ticker": t, "Action": action, "总分": v50['score'], 
                    "RS线": "🌟先行" if v50['rs_nh'] else "-", "加速": "仰攻📈" if v50['acceleration'] > 0 else "走平",
                    "移动止盈": v50['trailing_stop'], "POC支撑": v50['poc'], "Price": close, "紧致度": v50['tightness']
                })
        except: continue

    # 大盘天气
    breadth = ma50_up_count / total_valid if total_valid > 0 else 0
    weather = "☀️ 极佳" if (breadth > 0.6 and vix < 22) else "⛈️ 风险" if (breadth < 0.4 or vix > 28) else "☁️ 震荡"

    seeds = sorted(pre_candidates, key=lambda x: x['总分'], reverse=True)[:5]

    print(f"🔥 [3/3] 期权哨兵核验 (Polygon 限速延迟)...")
    results = []
    for item in seeds:
        opt_score, opt_desc = get_sentiment_v50(item['Ticker'])
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

    output_v50_to_sheets(results, weather, breadth, vix)

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

def output_v50_to_sheets(res, weather, breadth, vix):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        header_area = [
            ["🏰 [V50 天基指挥部终极版]", "", "Update:", datetime.datetime.now().strftime('%m-%d %H:%M')],
            ["环境天气:", weather, "大盘宽度:", f"{round(breadth*100, 1)}%", "VIX:", round(vix, 2)],
            ["操作建议:", "关注【👁️暗盘先行】感知权重股调仓，关注【🚀垂直爆破】感知CF/PR起爆。"],
            ["", "", "", ""]
        ]
        
        if res:
            df = pd.DataFrame(res)
            # 定义显示的列
            cols = ["Ticker", "评级", "Action", "RS线", "加速", "移动止盈", "POC支撑", "财报", "Price", "期权看涨%", "期权规模", "紧致度"]
            df = df[cols]
            
            # --- 终极净化流程 ---
            final_matrix = purify_for_google_sheets(df)
            
            sh.update(values=header_area, range_name="A1")
            sh.update(values=final_matrix, range_name="A5")
        else:
            sh.update(values=header_area, range_name="A1")
            sh.update_acell("A5", "今日无符合信号。")
            
        print("🎉 V50 任务圆满完成！数据净化通过。")
    except Exception as e:
        print(f"❌ 最终写入失败 (严重): {e}")

if __name__ == "__main__":
    run_v50_citadel()
