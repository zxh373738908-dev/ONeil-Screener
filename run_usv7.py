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

# 🛡️ 强制忽略 yfinance 内部警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
POLYGON_API_KEY = "您的_POLYGON_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)

SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_SHEET_NAME = "us Screener" 
CREDS_FILE = "credentials.json"
ACCOUNT_SIZE = 10000

# ==========================================
# 2. 核心工具
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

# ==========================================
# 3. V1000 枢纽引擎 (提早感知逻辑)
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
        # 奇點先行感知 (GOOGL 调仓信号)
        rs_nh_20 = rs_line.iloc[-1] >= rs_line.tail(20).max()
        # 紧致度 (Minervini VCP 核心)
        tightness = (close.tail(10).std() / close.tail(10).mean()) * 100
        
        def get_perf(d): 
            if len(close) < d+1: return 0
            return (curr_price - close.iloc[-d]) / close.iloc[-d]
        rs_score = (get_perf(63)*2 + get_perf(126) + get_perf(189) + get_perf(252))

        signals = []
        base_res = 0
        
        # 1. 👁️ 奇點先行 (RS线新高 + 窄幅收缩)
        if rs_nh_20 and tightness < 1.4:
            signals.append("👁️奇點先行")
            base_res += 3
            
        # 2. 🚀 巔峰突破 (CF 资源爆发信号)
        high_52w = high.tail(252).max()
        if curr_price >= high_52w * 0.98 and vol.iloc[-1] > vol_ma50:
            signals.append("🚀巔峰突破")
            base_res += 2
            
        # 3. 🐉 老龍回頭
        if rs_score > 0.5 and abs(curr_price - ma50)/ma50 < 0.03 and vol.iloc[-1] < vol_ma50 * 0.7:
            signals.append("🐉老龍回頭")
            base_res += 2

        if not signals: return None

        adr = ((high - low) / low).tail(20).mean() * 100
        
        return {
            "RS_Score": rs_score, "Signals": signals, "Base_Res": base_res, 
            "Price": curr_price, "Tightness": tightness, "ADR": adr, "RS_NH": rs_nh_20
        }
    except: return None

# ==========================================
# 4. 主指挥引擎
# ==========================================
def run_v1000_nexus_command():
    print(f"🏟️ [V1000] 枢纽系统 4.0 绝缘版启动...")
    
    # 🛡️ 策略升级：分步下载，增加重试逻辑，禁用多线程
    try:
        print("📡 正在获取大盘基准数据 (SPY/VIX)...")
        # 第一次尝试：不使用多线程，防止路径报错
        env = yf.download(["SPY", "^VIX"], period="2y", progress=False, threads=False)
        
        if env.empty or 'SPY' not in env['Close'].columns:
            raise ValueError("SPY 数据下载失败")
            
        spy_df = env['Close']['SPY'].dropna()
        vix = env['Close']['^VIX'].iloc[-1]
        spy_healthy = spy_df.iloc[-1] > spy_df.rolling(50).mean().iloc[-1]
    except Exception as e:
        print(f"❌ 核心环境数据失效: {e}")
        return

    # 获取标的
    try:
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        tickers = list(set(sp500['Symbol'].str.replace('.', '-').tolist() + ["TSLA", "PLTR", "MSTR", "NVDA", "CF", "PR", "GOOGL", "CEG"]))
        sector_map = dict(zip(sp500['Symbol'].str.replace('.', '-'), sp500['GICS Sector']))
    except:
        tickers = ["NVDA", "TSLA", "CF", "PR", "GOOGL"]; sector_map = {}

    print(f"🚀 正在对 {len(tickers)} 只标的执行天基演算...")
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
        print("📭 战区沉寂，未发现共振信号。")
        write_to_sheets([], vix, spy_healthy)
        return

    # 集群计算与终审
    for c in candidates:
        c["Total_Res"] = c["Base_Res"] + min(sector_cluster.get(c["Sector"], 1) - 1, 3)

    final_df = pd.DataFrame(candidates).sort_values(by=["Total_Res", "RS_Score"], ascending=False).head(12)
    results = []
    
    print(f"🔥 执行【V1000】末端期权流审计...")
    for _, row in final_df.iterrows():
        # 获取期权流数据 (Polygon API)
        opt_status = "平稳"
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
        time.sleep(13) # Polygon 频率保护

    write_to_sheets(results, vix, spy_healthy)

def write_to_sheets(results, vix, spy_healthy):
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
            ["🏰 V1000 枢纽指挥部 [4.0绝缘版]", "", "Update(BJ):", bj_time],
            ["大盘天气:", "☀️ 极佳" if (spy_healthy and vix < 21) else "⛈️ 避险", "VIX:", round(vix, 2), "SPY:", "健康" if spy_healthy else "弱势"],
            ["作战重点:", "👁️奇點先行(感知GOOGL调仓), 🚀巔峰突破(感知CF起爆), 💎SSS(双重共振)"],
            []
        ]
        sh.update(range_name="A1", values=header)

        if results:
            df = pd.DataFrame(results)
            clean_vals = [df.columns.tolist()] + [[robust_json_clean(item) for item in row] for row in df.values.tolist()]
            sh.update(range_name="A5", values=clean_vals)
        else:
            sh.update(range_name="A5", values=[["📭 暂无符合共振特征的信号。"]])
        print("🎉 指挥部战报已上传 Google Sheets。")
    except Exception as e: print(f"❌ 最终同步失败: {e}")

if __name__ == "__main__":
    run_v1000_nexus_command()
