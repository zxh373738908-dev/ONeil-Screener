import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import time
import math
import requests
import json
from polygon import RESTClient

# 🛡️ 屏蔽一切干扰
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
POLYGON_API_KEY = "您的_POLYGON_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)

SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_SHEET_NAME = "us Screener" 
CREDS_FILE = "credentials.json"

# ==========================================
# 2. 潜龙数据净化引擎
# ==========================================
def hidden_dragon_clean(val):
    """
    终极净化：确保数据绝对符合 JSON 标准，且彻底脱离 Numpy 依赖
    """
    if val is None: return ""
    # 检查 Numpy 类型
    if isinstance(val, (np.floating, float)):
        if not math.isfinite(val): return 0.0
        return float(round(val, 3))
    if isinstance(val, (np.integer, int)):
        return int(val)
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.strftime('%Y-%m-%d')
    
    # 强制转为字符串并剔除潜在的非 JSON 字符
    res = str(val).strip()
    return res if res != "nan" else ""

def safe_div(n, d):
    try:
        n_f, d_f = float(n), float(d)
        return n_f / d_f if d_f != 0 and math.isfinite(n_f) and math.isfinite(d_f) else 0.0
    except: return 0.0

# ==========================================
# 3. 枢纽战法引擎 (深度感知 GOOGL/CF)
# ==========================================
def calculate_v1000_nexus(df, spy_df):
    try:
        if len(df) < 150: return None
        close, vol, high, low = df['Close'], df['Volume'], df['High'], df['Low']
        curr_price = close.iloc[-1]
        
        # 均线与成交量
        ma50 = close.rolling(50).mean().iloc[-1]
        vol_ma50 = vol.rolling(50).mean().iloc[-1]

        # 相对强度 (RS) 枢纽
        rs_line = close / spy_df
        rs_nh_20 = rs_line.iloc[-1] >= rs_line.tail(20).max()
        
        # 紧致度算法 (Minervini VCP)
        tightness = (close.tail(10).std() / (close.tail(10).mean() + 0.0001)) * 100
        
        # 绩效计算
        def get_perf(d): 
            if len(close) < d+1: return 0
            return (curr_price - close.iloc[-d]) / (close.iloc[-d] + 0.0001)
        rs_score = (get_perf(63)*2 + get_perf(126) + get_perf(189) + get_perf(252))

        signals = []
        base_res = 0
        
        # 1. 👁️ 奇點先行 (感知 GOOGL：RS线先破+紧致度 < 1.3)
        if rs_nh_20 and tightness < 1.3:
            signals.append("👁️奇點先行")
            base_res += 3
            
        # 2. 🚀 巔峰突破 (感知 CF：新高附近+放量冲击)
        high_52w = high.tail(252).max()
        if curr_price >= high_52w * 0.98 and vol.iloc[-1] > vol_ma50 * 1.15:
            signals.append("🚀巔峰突破")
            base_res += 2
            
        # 3. 🐉 老龍回頭
        if rs_score > 0.5 and abs(curr_price - ma50)/ma50 < 0.03 and vol.iloc[-1] < vol_ma50 * 0.7:
            signals.append("🐉老龍回頭")
            base_res += 2

        if not signals: return None

        adr = ((high - low) / (low + 0.0001)).tail(20).mean() * 100
        
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr, "RS_NH": rs_nh_20
        }
    except: return None

# ==========================================
# 4. 主指挥引擎：执行扫描
# ==========================================
def run_v1000_nexus_command():
    print(f"🏟️ [V1000] 枢纽系统 8.0 潜龙加固版启动...")
    
    # 1. 大盘基准
    try:
        env = yf.download(["SPY", "^VIX"], period="2y", progress=False, threads=False)
        spy_df = env['Close']['SPY'].dropna()
        vix = env['Close']['^VIX'].iloc[-1]
        spy_healthy = spy_df.iloc[-1] > spy_df.rolling(50).mean().iloc[-1]
    except Exception as e:
        print(f"❌ 基准下载失败: {e}"); return

    # 2. 获取标的 (Wikipedia 隧道)
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers=headers, timeout=15)
        sp_df = pd.read_html(resp.text)[0]
        tickers = sp_df['Symbol'].str.replace('.', '-').tolist()
        sector_map = dict(zip(sp_df['Symbol'].str.replace('.', '-'), sp_df['GICS Sector']))
        tickers = list(set(tickers + ["NVDA", "TSLA", "PLTR", "CF", "PR", "GOOGL"]))
    except:
        tickers = ["NVDA", "TSLA", "PLTR", "CF", "PR", "GOOGL"]; sector_map = {}

    # 3. 核心扫描
    print(f"🚀 正在对 {len(tickers)} 只标的执行枢纽感知演算...")
    data = yf.download(tickers, period="2y", group_by='ticker', threads=True, progress=False)
    
    candidates = []
    sector_cluster = {}

    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df_t = data[t].dropna()
            if len(df_t) < 120: continue
            
            res = calculate_v1000_nexus(df_t, spy_df)
            if res:
                res.update({"Ticker": t, "Sector": sector_map.get(t, "Leaders")})
                candidates.append(res)
                s = res["Sector"]; sector_cluster[s] = sector_cluster.get(s, 0) + 1
        except: continue

    if not candidates:
        sync_to_sheets_chunked([], vix, spy_healthy)
        return

    # 4. 集群效应加成
    for c in candidates:
        c["Total_Res"] = c["Base_Res"] + min(sector_cluster.get(c["Sector"], 1) - 1, 3)

    # 5. 期权审计
    final_df = pd.DataFrame(candidates).sort_values(by=["Total_Res", "RS_Score"], ascending=False).head(15)
    results = []
    
    print(f"🔥 执行【期权流】枢纽审计...")
    for _, row in final_df.iterrows():
        call_pct = 50.0
        try:
            snaps = client_poly.get_snapshot_options_chain(row['Ticker'])
            c_val, p_val, sweep = 0, 0, 0
            for s in snaps:
                v = s.day.volume if (s.day and s.day.volume) else 0
                if v < 100: continue
                oi = s.open_interest if s.open_interest else 1
                if v / oi > 2.2: sweep += 1
                val = v * (s.day.last or 0) * 100
                if s.details.contract_type == 'call': c_val += val
                else: p_val += val
            call_pct = round(safe_div(c_val, (c_val + p_val)) * 100, 1)
        except: pass

        rating = "🔥强势"
        if row['Total_Res'] >= 5 and call_pct > 65: rating = "💎SSS 枢纽共振"
        elif row['Total_Res'] >= 4: rating = "👑SS 集群联动"

        results.append({
            "Ticker": row['Ticker'], "评级": rating, "信号": " + ".join(row['Signals']),
            "集群": f"{sector_cluster.get(row['Sector'], 1)}只异动",
            "看涨%": f"{call_pct}%", "Price": row['Price'],
            "枢纽分": row['Total_Res'], "紧致度": f"{round(row['Tightness'],2)}%",
            "板块": row['Sector']
        })
        time.sleep(12.5) # 避开 Polygon 限制

    sync_to_sheets_chunked(results, vix, spy_healthy)

# ==========================================
# 5. 潜龙切片同步引擎 (核心修复)
# ==========================================
def sync_to_sheets_chunked(results, vix, spy_healthy):
    print("📝 正在激活潜龙切片同步引擎...")
    
    # 1. 构建状态栏 (Chunk 1)
    bj_time = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    header_matrix = [
        ["🏰 V1000 终极枢纽 [8.0潜龙加固版]", "", "Update(BJ):", bj_time],
        ["市场环境:", "☀️ 激进" if (spy_healthy and vix < 21) else "⛈️ 避险", "VIX:", round(float(vix), 2), "SPY:", "健康" if spy_healthy else "弱势"],
        ["感知核心:", "👁️奇點先行(感知GOOGL/权重), 🚀巔峰突破(感知CF/资源), 💎SSS(终极共振)"],
        ["-" * 12] * 9
    ]
    
    # 2. 构建数据体 (Chunk 2)
    if results:
        df = pd.DataFrame(results)
        body_matrix = [df.columns.tolist()] + df.values.tolist()
    else:
        body_matrix = [["📭 当前战区进入静默期，无共振信号。"]]

    # 3. 执行分片同步
    for attempt in range(3):
        try:
            creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(SHEET_ID).worksheet(TARGET_SHEET_NAME)
            
            # 第一步：只更新状态栏 (减少 API 单次负载)
            clean_header = [[hidden_dragon_clean(c) for c in r] for r in header_matrix]
            sh.update('A1', clean_header)
            time.sleep(2) # 战术停顿
            
            # 第二步：覆盖更新数据体
            clean_body = [[hidden_dragon_clean(c) for c in r] for r in body_matrix]
            # 为了彻底防止 char 1 错误，如果 body 太大，我们进一步压缩
            sh.update('A5', clean_body)
            
            print(f"🎉 潜龙同步达成！第 {attempt + 1} 次尝试成功。")
            return
            
        except Exception as e:
            print(f"⚠️ 同步中断 (尝试 {attempt + 1}/3): {e}")
            time.sleep(25) # 遇到 char 1 必须大幅度退避

if __name__ == "__main__":
    run_v1000_nexus_command()
