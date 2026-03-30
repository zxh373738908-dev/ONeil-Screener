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
# 🛡️ 核心工具：数据安全清洗 (防止 JSON 报错)
# ==========================================
def deep_clean_data(val):
    """递归清理所有非标准数值，确保 JSON 兼容"""
    if isinstance(val, float):
        if np.isinf(val) or np.isnan(val):
            return 0.0
        return round(val, 3)
    if isinstance(val, dict):
        return {k: deep_clean_data(v) for k, v in val.items()}
    if isinstance(val, list):
        return [deep_clean_data(v) for v in val]
    return val

def safe_div(n, d):
    """安全除法"""
    if d == 0 or np.isnan(d) or np.isinf(d):
        return 0.0
    res = n / d
    return res if not (np.isnan(res) or np.isinf(res)) else 0.0

# ==========================================
# 2. V50 核心算法库
# ==========================================
def calculate_v50_metrics(df, spy_df):
    try:
        close = df['Close']
        vol = df['Volume']
        
        # 1. 垂直加速度 (Verticality)
        rs_line = (close / spy_df['Close']).fillna(method='ffill')
        
        # 计算斜率偏转
        slope_now = safe_div(rs_line.iloc[-1] - rs_line.iloc[-6], rs_line.iloc[-6])
        slope_prev = safe_div(rs_line.iloc[-7] - rs_line.iloc[-12], rs_line.iloc[-12])
        acceleration = slope_now - slope_prev
        
        # 2. 紧致度 (Tightness)
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean())
        
        # 3. U/D 量能比
        up_v = vol[df['Close'] > df['Open']].tail(40).sum()
        dn_v = vol[df['Close'] < df['Open']].tail(40).sum()
        ud_ratio = safe_div(up_v, dn_v)
        
        # 4. ATR 动态移动止盈
        tr = pd.concat([
            (df['High'] - df['Low']), 
            (df['High'] - close.shift(1)).abs(), 
            (df['Low'] - close.shift(1)).abs()
        ], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        atr_val = atr if (not np.isnan(atr) and not np.isinf(atr)) else close.iloc[-1] * 0.03
        trailing_stop = close.iloc[-1] - (2.5 * atr_val)
        
        # 5. 筹码中心 POC
        counts, bin_edges = np.histogram(close.tail(120), bins=50, weights=vol.tail(120))
        poc = (bin_edges[np.argmax(counts)] + bin_edges[np.argmax(counts)+1]) / 2
        
        # 6. 综合强度
        rs_raw = safe_div(close.iloc[-1] / close.iloc[-63], spy_df['Close'].iloc[-1] / spy_df['Close'].iloc[-63])
        score = rs_raw * ud_ratio * safe_div(1, (tightness * 100))
        
        return {
            "score": score, "acceleration": acceleration, "tightness": round(tightness*100, 3),
            "ud_ratio": round(ud_ratio, 2), "trailing_stop": round(trailing_stop, 2),
            "poc": round(poc, 2), "rs_raw": round(rs_raw, 2),
            "dist_high": safe_div(close.iloc[-1] - df['High'].max(), df['High'].max())
        }
    except:
        return None

# ==========================================
# 3. 选股扫描引擎
# ==========================================
def run_v50_citadel():
    print("📡 [1/3] 天基指挥部启动：正在执行全球宏观共振与加速度分析...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        sp_tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)
        raw_list = sp_tables[0]['Symbol'].tolist()
        # 修正 Ticker 符号格式
        tickers = [t.replace('.', '-') for t in raw_list]
        tickers = list(set(tickers + ["PR", "CF", "NTR", "FANG", "NVDA", "GOOGL"]))
    except Exception as e:
        print(f"❌ 获取名册失败: {e}")
        return

    # 获取宏观环境
    env = yf.download(["DX-Y.NYB", "^VIX", "SPY"], period="5d", progress=False)['Close']
    vix = env["^VIX"].iloc[-1] if ("^VIX" in env.columns and not env.empty) else 20
    
    # 下载核心数据
    data = yf.download(tickers + ["SPY"], period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"].dropna()

    pre_candidates = []
    ma50_up_count = 0

    print(f"🚀 [2/3] 正在对 {len(tickers)} 个目标执行‘垂直加速度’演算...")
    
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 200: continue
            
            close = df['Close'].iloc[-1]
            # 修正 iloc 语法错误
            ma50_val = df['Close'].tail(50).mean()
            if close > ma50_val: ma50_up_count += 1
            
            # 基础过滤：只要二阶段上升趋势
            if close < df['Close'].tail(200).mean(): continue
            
            v50 = calculate_v50_metrics(df, spy_df)
            if not v50: continue
            
            # 模式识别
            is_explosion = (v50['dist_high'] >= -0.05) and (v50['acceleration'] > 0)
            is_dip = (0 <= (close - v50['poc'])/v50['poc'] <= 0.06) and (v50['rs_raw'] > 1.1)
            
            if is_explosion or is_dip:
                pre_candidates.append({
                    "Ticker": t, 
                    "Action": "🚀垂直爆破" if is_explosion else "🐉支撑回踩",
                    "总分": v50['score'], 
                    "加速趋势": "仰攻📈" if v50['acceleration'] > 0 else "走平",
                    "紧致度": v50['tightness'], 
                    "U/D比": v50['ud_ratio'], 
                    "移动止盈": v50['trailing_stop'],
                    "POC支撑": v50['poc'], 
                    "Price": round(close, 2)
                })
        except: continue

    # 计算大盘天气
    breadth = ma50_up_count / len(tickers) if len(tickers) > 0 else 0
    weather = "☀️ 极佳" if (breadth > 0.6 and vix < 22) else "⛈️ 风险" if (breadth < 0.4 or vix > 28) else "☁️ 震荡"

    # 排序筛选前 5
    seeds = sorted(pre_candidates, key=lambda x: x['总分'], reverse=True)[:5]

    print(f"🔥 [3/3] 调动期权雷达执行最终核验 (Polygon 限速延迟)...")
    results = []
    for item in seeds:
        opt_score, opt_desc = get_sentiment_v50(item['Ticker'])
        try:
            t_obj = yf.Ticker(item['Ticker'])
            cal = t_obj.calendar
            if cal is not None and not cal.empty:
                days_to_e = (cal.iloc[0, 0].date() - datetime.date.today()).days
                eb_str = "⚠️临近" if 0 <= days_to_e <= 7 else f"{days_to_e}d"
            else:
                eb_str = "未知"
        except: eb_str = "未知"
        
        item.update({
            "评级": "💎SSS+" if (opt_score > 65) else "🔥强势",
            "财报": eb_str, 
            "期权": f"{opt_score}% Call", 
            "规模": opt_desc
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
        return round(safe_div(bull, total)*100, 1) if total > 0 else 50, f"${round(total/1e6, 2)}M"
    except: return 50, "N/A"

def output_v50_to_sheets(res, weather, breadth, vix):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        status_bar = [
            ["🏰 [V50 天基指挥部终极版]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["环境天气:", weather, "大盘宽度:", f"{round(breadth*100, 1)}%", "VIX:", round(vix, 2)],
            ["操作建议:", "移动止盈是生命线。不破该位，死抱盈利让利润奔跑！"],
            ["", "", "", ""]
        ]
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "加速趋势", "移动止盈", "POC支撑", "财报", "Price", "期权", "规模", "紧致度"]
            df = df[cols]
            
            # --- 关键：最后一次深度清洗，确保 JSON 100% 兼容 ---
            raw_matrix = [df.columns.tolist()] + df.values.tolist()
            final_matrix = deep_clean_data(raw_matrix)
            
            sh.update(values=status_bar, range_name="A1")
            sh.update(values=final_matrix, range_name="A5")
        else:
            sh.update(values=status_bar, range_name="A1")
            sh.update_acell("A5", "今日无符合信号标的。")
            
        print("🎉 V50 天基任务执行完毕，情报已送达指挥中心！")
    except Exception as e:
        print(f"❌ 最终写入失败: {e}")

if __name__ == "__main__":
    run_v50_citadel()
