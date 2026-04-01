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

# 🛡️ 核心修复：禁用 yfinance 的本地数据库缓存，防止 GitHub Actions 环境下出现 database locked
import yfinance.utils as yf_utils
try:
    yf.set_tz_cache_location(None) # 彻底解决 database is locked 问题
except:
    pass

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心
# ==========================================
POLYGON_API_KEY = "您的_POLYGON_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)

SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
TARGET_SHEET_NAME = "us Screener" # 请确保 Google Sheet 中标签页名字完全一致
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
# 3. V1000 枢纽引擎 (包含提早感知逻辑)
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
        tightness = (close.tail(10).std() / close.tail(10).mean()) * 100
        
        # IBD RS 评分
        def get_perf(d): 
            if len(close) < d+1: return 0
            return (curr_price - close.iloc[-d]) / close.iloc[-d]
        rs_score = (get_perf(63)*2 + get_perf(126) + get_perf(189) + get_perf(252))

        signals = []
        base_res = 0
        
        # 1. 👁️ 奇點先行 (提早感知 GOOGL 等大票)
        if rs_nh_20 and tightness < 1.3:
            signals.append("👁️奇點先行")
            base_res += 3
            
        # 2. 🚀 巔峰突破 (捕捉 CF 起爆点)
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

def get_option_nexus(ticker):
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
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
        return ("🔥扫货" if sweep >= 2 else "活跃" if call_pct > 60 else "平稳"), call_pct
    except: return "N/A", 50.0

# ==========================================
# 4. 主程序
# ==========================================
def run_v1000_nexus_command():
    print(f"🏟️ [V1000] 枢纽系统 3.0 启动: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 强制单线程下载基准数据，防止 locked 报错
    env = yf.download(["SPY", "^VIX"], period="2y", progress=False, threads=False)['Close']
    if 'SPY' not in env.columns: 
        print("❌ 核心数据缺失，停止。"); return
    
    spy_df = env['SPY'].dropna()
    vix = env['^VIX'].iloc[-1]
    spy_healthy = spy_df.iloc[-1] > spy_df.rolling(50).mean().iloc[-1]
    
    try:
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        tickers = list(set(sp500['Symbol'].str.replace('.', '-').tolist() + ["TSLA", "PLTR", "MSTR", "NVDA", "CF", "PR", "GOOGL"]))
        sector_map = dict(zip(sp500['Symbol'].str.replace('.', '-'), sp500['GICS Sector']))
    except:
        tickers = ["NVDA", "TSLA", "CF", "PR", "GOOGL"]; sector_map = {}

    data = yf.download(tickers, period="2y", group_by='ticker', threads=True, progress=False)
    candidates = []
    sector_cluster = {}

    print(f"🚀 正在扫描全战区 (共 {len(tickers)} 只)...")
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
        print("📭 未发现符合信号的标的。")
        write_to_sheets([], vix, spy_healthy)
        return

    # 集群计算与终审
    for c in candidates:
        c["Total_Res"] = c["Base_Res"] + min(sector_cluster.get(c["Sector"], 1) - 1, 3)

    final_df = pd.DataFrame(candidates).sort_values(by=["Total_Res", "RS_Score"], ascending=False).head(12)
    results = []
    
    print(f"🔥 执行【期权+财报】终审...")
    for _, row in final_df.iterrows():
        opt_status, call_pct = get_option_nexus(row['Ticker'])
        try:
            t_obj = yf.Ticker(row['Ticker'])
            cal = t_obj.calendar
            earning_str = "N/A"
            if cal is not None and not cal.empty:
                earning_date = cal.iloc[0,0]
                days_to_e = (earning_date.date() - datetime.date.today()).days
                earning_str = f"{days_to_e}天后"
                if 0 <= days_to_e <= 7: earning_str = "⚠️避开财报"
        except: earning_str = "N/A"

        rating = "🔥强势"
        if row['Total_Res'] >= 5 and call_pct > 65: rating = "💎SSS 枢纽共振"
        elif row['Total_Res'] >= 4: rating = "👑SS 集群联动"

        results.append({
            "Ticker": row['Ticker'], "评级": rating, "共振战法": " + ".join(row['Signals']),
            "集群效应": f"{sector_cluster.get(row['Sector'], 1)}只异动",
            "期权状态": opt_status, "看涨%": f"{call_pct}%",
            "财报窗口": earning_str, "Price": row['Price'],
            "止损位": round(row['Price'] * (1 - row['ADR']*0.015), 2),
            "枢纽分": row['Total_Res'], "板块": row['Sector']
        })
        time.sleep(13)

    write_to_sheets(results, vix, spy_healthy)

def write_to_sheets(results, vix, spy_healthy):
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh_main = client.open_by_key(SHEET_ID)
        
        # 🛡️ 核心修复：如果标签页不存在，则自动创建，防止写入失败
        try:
            sh = sh_main.worksheet(TARGET_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            sh = sh_main.add_worksheet(title=TARGET_SHEET_NAME, rows="100", cols="20")

        sh.clear()
        bj_time = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M')
        header = [
            ["🏰 V1000 终极枢纽指挥部 [3.0强袭版]", "", "Update(BJ):", bj_time],
            ["当前环境:", "☀️ 激进" if (spy_healthy and vix < 20) else "☁️ 震荡", "VIX:", round(vix, 2), "SPY:", "健康" if spy_healthy else "弱势"],
            ["作战指南:", "寻找 💎SSS 且 集群效应 > 2 的标的 (CF/PR 属于此类)"],
            []
        ]
        sh.update(range_name="A1", values=header)

        if results:
            df = pd.DataFrame(results)
            clean_vals = [df.columns.tolist()] + [[robust_json_clean(item) for item in row] for row in df.values.tolist()]
            sh.update(range_name="A5", values=clean_vals)
        else:
            sh.update(range_name="A5", values=[["📭 暂无共振信号，空仓等待。"]])
        print("🎉 指挥中心同步完成。")
    except Exception as e: print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    run_v1000_nexus_command()
