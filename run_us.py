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
# 🛡️ 核心工具：数据净化 (100% 解决 JSON 兼容)
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
# 2. V100 核心演算逻辑 (Aegis Engine)
# ==========================================
def calculate_v100_metrics(df, spy_df):
    try:
        close = df['Close']
        high, low, vol = df['High'], df['Low'], df['Volume']
        
        # 1. 三周期趋势模板 (Trend Template)
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        ma200 = close.rolling(200).mean()
        # 强制 Stage 2 逻辑
        is_stage_2 = bool(close.iloc[-1] > ma50.iloc[-1] > ma150.iloc[-1] > ma200.iloc[-1])
        
        # 2. RS 综合强度 (3个月 + 6个月加权)
        rs_line = (close / spy_df).fillna(method='ffill')
        rs_3m = safe_div(rs_line.iloc[-1], rs_line.iloc[-63])
        rs_6m = safe_div(rs_line.iloc[-1], rs_line.iloc[-126])
        rs_score = (rs_3m * 0.7) + (rs_6m * 0.3)
        
        # RS 加速度 (加速仰攻)
        accel = safe_div(rs_line.iloc[-1] - rs_line.iloc[-10], rs_line.iloc[-10]) - \
                safe_div(rs_line.iloc[-11] - rs_line.iloc[-20], rs_line.iloc[-20])
        
        # 3. 紧致度 (Tightness) 与 缩量感知 (V-Dry)
        tightness = safe_div(close.tail(10).std(), close.tail(10).mean()) * 100
        avg_vol = vol.tail(10).mean()
        is_vdry = bool(vol.iloc[-1] < avg_vol * 0.65) # 极度缩量
        
        # 4. 筹码中心 POC
        counts, bin_edges = np.histogram(close.tail(120).values, bins=50, weights=vol.tail(120).values)
        poc = (bin_edges[np.argmax(counts)] + bin_edges[np.argmax(counts)+1]) / 2
        
        # 5. 动态止盈
        atr = (high - low).rolling(14).mean().iloc[-1]
        trailing_stop = close.iloc[-1] - (2.5 * (atr if math.isfinite(atr) else close.iloc[-1]*0.03))
        
        # 综合评分 (分值权重：相对强度 > 量能 > 紧致度)
        total_score = (rs_score * 50) + (accel * 500) + (10 if is_vdry else 0)
        
        return {
            "score": total_score, "is_stage_2": is_stage_2, "accel": accel,
            "rs_rank": rs_score, "tightness": tightness, "v_dry": is_vdry,
            "stop": trailing_stop, "poc": poc, "price": close.iloc[-1]
        }
    except: return None

# ==========================================
# 3. 选股扫描引擎
# ==========================================
def run_v100_aegis():
    print("📡 [1/3] 天基指挥部 V100：正在同步全球领袖与行业因子...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 1. 获取标普 500 名册与板块映射
    try:
        sp_tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)
        sp_df = sp_tables[0]
        ticker_sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(ticker_sector_map.keys()) + ["PR", "CF", "NTR", "NVDA", "GOOGL", "PLTR"]
    except:
        tickers = ["NVDA", "GOOGL", "AAPL", "MSFT", "CF", "PR"]; ticker_sector_map = {}

    # 2. 批量数据下载
    data = yf.download(list(set(tickers + ["SPY", "^VIX"])), period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"]["Close"].dropna()
    vix = float(data["^VIX"]["Close"].dropna().iloc[-1]) if "^VIX" in data.columns else 20.0

    # 3. 计算市场宽度 (MA50 上方占比)
    above_50ma, valid_count = 0, 0
    for t in tickers[:200]:
        if t in data.columns.levels[0]:
            df = data[t]["Close"].dropna()
            if len(df) > 50 and df.iloc[-1] > df.tail(50).mean(): above_50ma += 1
            valid_count += 1
    breadth = (above_50ma / valid_count) * 100 if valid_count > 0 else 50

    print(f"🚀 [2/3] 执行‘Aegis 绝对防御’演算 (宽度: {breadth:.1f}%)...")
    candidates = []
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 250: continue
            
            v100 = calculate_v100_metrics(df, spy_df)
            if not v100 or not v100['is_stage_2']: continue
            
            # --- 动作分类判定 ---
            action = "关注"
            if v100['accel'] > 0.005 and v100['tightness'] < 1.5: action = "👁️暗盘先行"
            elif v100['v_dry'] and v100['rs_rank'] > 1.1: action = "🐉缩量回踩"
            elif v100['accel'] > 0.01: action = "🚀垂直爆破"
            
            if action == "关注": continue

            candidates.append({
                "Ticker": t, "Action": action, "Score": v100['score'], "Sector": ticker_sector_map.get(t, "Other"),
                "Price": v100['price'], "止损位": v100['stop'], "POC支撑": v100['poc'],
                "RS加速": "📈" if v100['accel'] > 0 else "-", "紧致度": v100['tightness']
            })
        except: continue

    if not candidates:
        final_output([], "⛈️ 风险", 0, vix); return

    # --- 核心改进：行业配额过滤 (每个行业最多入选 2 个最高分) ---
    cand_df = pd.DataFrame(candidates).sort_values(by="Score", ascending=False)
    final_seeds = cand_df.groupby("Sector").head(2).sort_values(by="Score", ascending=False).head(8)

    print(f"🔥 [3/3] 正在接入 Polygon 期权暗盘审计...")
    results = []
    weather = "☀️ 极佳" if (breadth > 60 and vix < 21) else "⛈️ 严寒" if (breadth < 40 or vix > 28) else "☁️ 阴天"

    for _, row in final_seeds.iterrows():
        opt_score, opt_size = get_sentiment(row['Ticker'])
        try:
            t_obj = yf.Ticker(row['Ticker'])
            cal = t_obj.calendar
            eb_str = "⚠️临近" if (cal is not None and not cal.empty and (cal.iloc[0, 0].date() - datetime.date.today()).days <= 7) else "安全"
        except: eb_str = "未知"

        row_dict = row.to_dict()
        row_dict.update({
            "财报": eb_str, "期权看涨%": opt_score, "期权规模": opt_size,
            "评级": "💎SSS+" if (opt_score > 65 and weather == "☀️ 极佳") else "🔥强势"
        })
        results.append(row_dict)
        time.sleep(12.5) # 遵守 Polygon 免费版 5次/min 限制

    final_output(results, weather, breadth, vix)

def get_sentiment(ticker):
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        bull, total = 0, 0
        for s in snaps:
            val = (s.day.volume or 0) * (s.day.last or 0) * 100
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
            ["🏰 [V100 天基指挥部 - 绝对防御版]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["环境天气:", weather, "大盘宽度:", f"{round(breadth, 1)}%", "VIX:", round(vix, 2)],
            ["防御策略:", "单一行业限额 2 只。强制 Stage 2 趋势过滤。优先关注【👁️暗盘先行】标的。"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "评级", "Action", "RS加速", "Price", "止损位", "POC支撑", "期权看涨%", "期权规模", "财报", "Sector", "紧致度"]
            df = df[[c for c in cols if c in df.columns]]
            raw_data = [df.columns.tolist()] + df.values.tolist()
            final_clean_matrix = [[robust_json_clean(cell) for cell in row] for row in raw_data]
            sh.update(values=final_clean_matrix, range_name="A5")
        else:
            sh.update_acell("A5", "📭 今日环境严寒，未探测到符合 Aegis 准则的阿尔法奇点。")
            
        print(f"🎉 V100 任务完成！当前天气：{weather}")
    except Exception as e: print(f"❌ 最终写入失败: {e}")

if __name__ == "__main__": run_v100_aegis()
