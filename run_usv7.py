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
from polygon import RESTClient

# 🛡️ 屏蔽杂音
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
# 2. 核心数据净化引擎 (星链版)
# ==========================================
def starlink_clean(val):
    """
    终极净化：物理切断 Numpy 依赖，确保 JSON 100% 兼容
    """
    if val is None or (isinstance(val, float) and not math.isfinite(val)):
        return ""
    if isinstance(val, (float, np.floating)):
        return float(round(val, 3))
    if isinstance(val, (int, np.integer)):
        return int(val)
    return str(val)

def safe_div(n, d):
    try:
        return n / d if d != 0 and math.isfinite(n) and math.isfinite(d) else 0.0
    except: return 0.0

# ==========================================
# 3. 枢纽战法引擎 (深度感知 GOOGL/CF)
# ==========================================
def calculate_v1000_nexus(df, spy_df):
    try:
        if len(df) < 150: return None
        close, vol, high, low = df['Close'], df['Volume'], df['High'], df['Low']
        curr_price = close.iloc[-1]
        
        ma50 = close.rolling(50).mean().iloc[-1]
        vol_ma50 = vol.rolling(50).mean().iloc[-1]

        # 相对强度 (RS) 枢纽
        rs_line = close / spy_df
        rs_nh_20 = rs_line.iloc[-1] >= rs_line.tail(20).max()
        
        # 紧致度 (Minervini VCP) - 越小越好
        tightness = (close.tail(10).std() / (close.tail(10).mean() + 0.001)) * 100
        
        def get_perf(d): 
            if len(close) < d+1: return 0
            return (curr_price - close.iloc[-d]) / (close.iloc[-d] + 0.001)
        rs_score = (get_perf(63)*2 + get_perf(126) + get_perf(189) + get_perf(252))

        signals = []
        base_res = 0
        
        # 1. 👁️ 奇點先行 (GOOGL大票逻辑：RS线新高 + 波动挤压)
        if rs_nh_20 and tightness < 1.3:
            signals.append("👁️奇點先行")
            base_res += 3
            
        # 2. 🚀 巔峰突破 (CF起爆逻辑：价格/成交量共振)
        high_52w = high.tail(252).max()
        if curr_price >= high_52w * 0.98 and vol.iloc[-1] > vol_ma50 * 1.1:
            signals.append("🚀巔峰突破")
            base_res += 2
            
        # 3. 🐉 老龍回頭
        if rs_score > 0.5 and abs(curr_price - ma50)/ma50 < 0.03 and vol.iloc[-1] < vol_ma50 * 0.7:
            signals.append("🐉老龍回頭")
            base_res += 2

        if not signals: return None

        adr = ((high - low) / (low + 0.001)).tail(20).mean() * 100
        
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr, "RS_NH": rs_nh_20
        }
    except: return None

# ==========================================
# 4. 主指挥引擎：天基演算
# ==========================================
def run_v1000_nexus_command():
    print(f"🏟️ [V1000] 枢纽系统 7.0 星链加固版启动...")
    
    # 1. 大盘基准
    try:
        env = yf.download(["SPY", "^VIX"], period="2y", progress=False, threads=False)
        spy_df = env['Close']['SPY'].dropna()
        vix = env['Close']['^VIX'].iloc[-1]
        spy_healthy = spy_df.iloc[-1] > spy_df.rolling(50).mean().iloc[-1]
    except:
        print("❌ 核心基准数据下载失败"); return

    # 2. 名册获取 (Wikipedia 隧道)
    print("📡 正在跨越 Wikipedia 墙体获取标普 500 名册...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers=headers, timeout=15)
        sp500_df = pd.read_html(resp.text)[0]
        tickers = sp500_df['Symbol'].str.replace('.', '-').tolist()
        sector_map = dict(zip(sp500_df['Symbol'].str.replace('.', '-'), sp500_df['GICS Sector']))
        tickers = list(set(tickers + ["TSLA", "PLTR", "MSTR", "NVDA", "CF", "PR", "GOOGL"]))
    except:
        tickers = ["NVDA", "TSLA", "CF", "PR", "GOOGL"]; sector_map = {}

    # 3. 演算循环
    print(f"🚀 正在对 {len(tickers)} 只标的执行共振审计...")
    data = yf.download(tickers, period="2y", group_by='ticker', threads=True, progress=False)
    
    candidates = []
    sector_cluster = {}

    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df_t = data[t].dropna()
            if len(df_t) < 100: continue
            
            res = calculate_v1000_nexus(df_t, spy_df)
            if res:
                res.update({"Ticker": t, "Sector": sector_map.get(t, "Leaders")})
                candidates.append(res)
                s = res["Sector"]; sector_cluster[s] = sector_cluster.get(s, 0) + 1
        except: continue

    if not candidates:
        write_to_sheets_with_retry([], vix, spy_healthy)
        return

    # 4. 集群加成
    for c in candidates:
        c["Total_Res"] = c["Base_Res"] + min(sector_cluster.get(c["Sector"], 1) - 1, 3)

    # 5. 期权终审 (Polygon 限速)
    final_df = pd.DataFrame(candidates).sort_values(by=["Total_Res", "RS_Score"], ascending=False).head(15)
    results = []
    
    print(f"🔥 执行【末端共振】期权流探测...")
    for _, row in final_df.iterrows():
        call_pct = 50.0
        try:
            snaps = client_poly.get_snapshot_options_chain(row['Ticker'])
            c_val, p_val, sweep = 0, 0, 0
            for s in snaps:
                v = s.day.volume if (s.day and s.day.volume) else 0
                if v < 100: continue
                oi = s.open_interest if s.open_interest else 1
                if v / oi > 2.5: sweep += 1
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
            "集群效应": f"{sector_cluster.get(row['Sector'], 1)}只异动",
            "看涨%": f"{call_pct}%", "Price": row['Price'],
            "枢纽分": row['Total_Res'], "紧致度": f"{round(row['Tightness'],2)}%",
            "板块": row['Sector']
        })
        time.sleep(13)

    write_to_sheets_with_retry(results, vix, spy_healthy)

# ==========================================
# 5. 星链重试同步引擎 (核心修复)
# ==========================================
def write_to_sheets_with_retry(results, vix, spy_healthy, max_retries=3):
    print("📝 正在激活星链重试同步引擎...")
    
    # 1. 准备大矩阵
    bj_time = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
    header = [
        ["🏰 V1000 终极枢纽 [7.0星链加固版]", "", "Update(BJ):", bj_time],
        ["市场环境:", "☀️ 激进" if (spy_healthy and vix < 21) else "⛈️ 避险", "VIX:", round(vix, 2), "SPY:", "健康" if spy_healthy else "弱势"],
        ["提早感知:", "👁️奇點先行(感知GOOGL/CF), 🚀巔峰突破(感知板块起爆), 💎SSS(终极共振)"],
        ["-" * 15] * 10
    ]
    
    data_body = []
    if results:
        df = pd.DataFrame(results)
        data_body = [df.columns.tolist()] + df.values.tolist()
    else:
        data_body = [["📭 今日战区沉寂，无符合信号的标的。"]]

    full_matrix = header + data_body
    # 终极净化
    clean_matrix = [[starlink_clean(cell) for cell in row] for row in full_matrix]

    # 2. 带有退避机制的重试循环
    for attempt in range(max_retries):
        try:
            # 每次重试重新认证，确保 Token 有效
            creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
            gc = gspread.authorize(creds)
            sh_main = gc.open_by_key(SHEET_ID)
            
            try:
                sh = sh_main.worksheet(TARGET_SHEET_NAME)
            except gspread.exceptions.WorksheetNotFound:
                sh = sh_main.add_worksheet(title=TARGET_SHEET_NAME, rows="100", cols="20")

            # 执行单次大矩阵写入
            sh.update('A1', clean_matrix)
            print(f"🎉 同步成功！第 {attempt + 1} 次尝试达成。")
            return
            
        except Exception as e:
            print(f"⚠️ 同步失败 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 20
                print(f"⏳ 正在执行战术退避，等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                print("❌ 星链同步引擎彻底失效，请检查 Google API 状态或网络环境。")

if __name__ == "__main__":
    run_v1000_nexus_command()
