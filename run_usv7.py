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
import traceback
from polygon import RESTClient

# 🛡️ 强制屏蔽所有杂讯
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
# 2. 核心工具与鲁棒性增强
# ==========================================
def robust_json_clean(val):
    if val is None or pd.isna(val): return ""
    if isinstance(val, (float, np.floating, np.float64)):
        return float(round(val, 3)) if math.isfinite(val) else 0.0
    return str(val)

def safe_div(n, d):
    try:
        return n / d if d != 0 and math.isfinite(n) and math.isfinite(d) else 0.0
    except: return 0.0

# 🛡️ 新增：防封锁的 Ticker 获取逻辑
def get_sp500_tickers_safe():
    print("📡 正在尝试通过安全通道获取标普 500 名册...")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        df = pd.read_html(response.text)[0]
        tickers = df['Symbol'].str.replace('.', '-').tolist()
        # 建立行业映射
        sector_map = dict(zip(df['Symbol'].str.replace('.', '-'), df['GICS Sector']))
        print(f"✅ 成功获取 {len(tickers)} 只标普 500 成分股。")
        return tickers, sector_map
    except Exception as e:
        print(f"⚠️ Wikipedia 访问受限: {e}。使用备用核心名单。")
        # 备用名单包含最核心的大票和活跃资源股
        backup = ["NVDA", "TSLA", "PLTR", "MSTR", "AMD", "CF", "PR", "NTR", "GOOGL", "AAPL", "MSFT", "AMZN", "META", "AVGO", "COST", "NFLX", "CEG", "VST"]
        return backup, {t: "Leaders" for t in backup}

# ==========================================
# 3. V1000 枢纽引擎算法
# ==========================================
def calculate_v1000_nexus(df, spy_df):
    try:
        if len(df) < 150: return None
        close, vol, high, low = df['Close'], df['Volume'], df['High'], df['Low']
        curr_price = close.iloc[-1]
        
        ma50 = close.rolling(50).mean().iloc[-1]
        vol_ma50 = vol.rolling(50).mean().iloc[-1]

        # 核心：相对强度 (RS) 线
        rs_line = close / spy_df
        rs_nh_20 = rs_line.iloc[-1] >= rs_line.tail(20).max()
        tightness = (close.tail(10).std() / (close.tail(10).mean() + 0.01)) * 100
        
        def get_perf(d): 
            if len(close) < d+1: return 0
            return (curr_price - close.iloc[-d]) / (close.iloc[-d] + 0.01)
        
        rs_score = (get_perf(63)*2 + get_perf(126) + get_perf(189) + get_perf(252))

        signals = []
        base_res = 0
        
        # 1. 👁️ 奇點先行 (提早感知 GOOGL 等大票)
        if rs_nh_20 and tightness < 1.4:
            signals.append("👁️奇點先行")
            base_res += 3
            
        # 2. 🚀 巔峰突破 (捕捉 CF 资源爆发信号)
        high_52w = high.tail(252).max()
        if curr_price >= high_52w * 0.975 and vol.iloc[-1] > vol_ma50 * 1.05:
            signals.append("🚀巔峰突破")
            base_res += 2
            
        # 3. 🐉 老龍回頭
        if rs_score > 0.4 and abs(curr_price - ma50)/ma50 < 0.035 and vol.iloc[-1] < vol_ma50 * 0.75:
            signals.append("🐉老龍回頭")
            base_res += 2

        if not signals: return None

        adr = ((high - low) / (low + 0.01)).tail(20).mean() * 100
        
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr, "RS_NH": rs_nh_20
        }
    except: return None

# ==========================================
# 4. 主指挥引擎
# ==========================================
def run_v1000_nexus_command():
    print(f"🏟️ [V1000] 枢纽系统 5.0 终极突围版启动...")
    
    # 🛡️ 强制单线程下载基准数据，确保成功
    try:
        env = yf.download(["SPY", "^VIX"], period="2y", progress=False, threads=False)
        if env.empty or 'SPY' not in env['Close'].columns:
            raise ValueError("SPY 数据缺失")
        spy_df = env['Close']['SPY'].dropna()
        vix = env['Close']['^VIX'].iloc[-1]
        spy_healthy = spy_df.iloc[-1] > spy_df.rolling(50).mean().iloc[-1]
    except Exception as e:
        print(f"❌ 大盘基准数据获取失败: {e}"); return

    # 获取全市场名册
    tickers, sector_map = get_sp500_tickers_safe()

    print(f"🚀 正在对 {len(tickers)} 只标的执行天基演算...")
    # 增加超时控制
    data = yf.download(tickers, period="2y", group_by='ticker', threads=True, progress=False, timeout=30)
    
    candidates = []
    sector_cluster = {}

    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df_t = data[t].dropna()
            if len(df_t) < 100: continue
            
            res = calculate_v1000_nexus(df_t, spy_df)
            if res:
                res.update({"Ticker": t, "Sector": sector_map.get(t, "Other")})
                candidates.append(res)
                s = res["Sector"]; sector_cluster[s] = sector_cluster.get(s, 0) + 1
        except: continue

    if not candidates:
        print("📭 战区沉寂，未发现符合共振形态的信号。")
        write_to_sheets_with_retry([], vix, spy_healthy)
        return

    # 集群计算
    for c in candidates:
        c["Total_Res"] = c["Base_Res"] + min(sector_cluster.get(c["Sector"], 1) - 1, 3)

    final_df = pd.DataFrame(candidates).sort_values(by=["Total_Res", "RS_Score"], ascending=False).head(15)
    results = []
    
    print(f"🔥 执行最终期权审计 (Polygon 限速模式)...")
    for _, row in final_df.iterrows():
        opt_status, call_pct = "平稳", 50.0
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
            opt_status = "🔥扫货" if sweep >= 2 else "活跃" if call_pct > 60 else "平稳"
        except: pass

        rating = "🔥强势"
        if row['Total_Res'] >= 5 and call_pct > 65: rating = "💎SSS 枢纽共振"
        elif row['Total_Res'] >= 4: rating = "👑SS 集群联动"

        results.append({
            "Ticker": row['Ticker'], "评级": rating, "信号": " + ".join(row['Signals']),
            "集群效应": f"{sector_cluster.get(row['Sector'], 1)}只同动",
            "看涨%": f"{call_pct}%", "Price": row['Price'],
            "枢纽分": row['Total_Res'], "紧致度": f"{round(row['Tightness'],2)}%",
            "板块": row['Sector']
        })
        time.sleep(13)

    write_to_sheets_with_retry(results, vix, spy_healthy)

# 🛡️ 新增：带重试机制的 Google Sheets 写入
def write_to_sheets_with_retry(results, vix, spy_healthy, retries=3):
    print("📝 正在同步战报至 Google 指挥中心...")
    for i in range(retries):
        try:
            creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
            client = gspread.authorize(creds)
            sh_main = client.open_by_key(SHEET_ID)
            
            try:
                sh = sh_main.worksheet(TARGET_SHEET_NAME)
            except gspread.exceptions.WorksheetNotFound:
                sh = sh_main.add_worksheet(title=TARGET_SHEET_NAME, rows="100", cols="20")

            sh.clear()
            bj_time = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
            header = [
                ["🏰 V1000 终极枢纽 [5.0终极突围版]", "", "Update(BJ):", bj_time],
                ["市场天气:", "☀️ 极佳" if (spy_healthy and vix < 21) else "⛈️ 避险", "VIX:", round(vix, 2), "SPY:", "健康" if spy_healthy else "弱势"],
                ["提早感知:", "👁️奇點(GOOGL大票信号), 🚀巔峰(CF资源爆发), 💎SSS(终极共振)"],
                []
            ]
            sh.update(range_name="A1", values=header)

            if results:
                df = pd.DataFrame(results)
                clean_vals = [df.columns.tolist()] + [[robust_json_clean(item) for item in row] for row in df.values.tolist()]
                sh.update(range_name="A5", values=clean_vals)
            else:
                sh.update(range_name="A5", values=[["📭 暂无信号，系统静默待命。"]])
            print("🎉 战报上传成功！")
            return
        except Exception as e:
            print(f"⚠️ 第 {i+1} 次写入尝试失败: {e}")
            if i < retries - 1: time.sleep(10)
            else: print("❌ 最终写入失败，请检查网络或 Google 服务状态。")

if __name__ == "__main__":
    run_v1000_nexus_command()
