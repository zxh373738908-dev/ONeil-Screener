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
POLYGON_API_KEY = "您的_POLYGON_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
CREDS_FILE = "credentials.json"
ACCOUNT_SIZE = 10000

# ==========================================
# 2. 核心算法：V1000 枢纽共振引擎
# ==========================================
def calculate_v1000_nexus(df, spy_df):
    try:
        if len(df) < 252: return None
        close, vol, high, low = df['Close'], df['Volume'], df['High'], df['Low']
        curr_price = close.iloc[-1]
        
        # --- 均线系统 ---
        ma10 = close.rolling(10).mean().iloc[-1]
        ma50 = close.rolling(50).mean().iloc[-1]
        ma200 = close.rolling(200).mean().iloc[-1]
        vol_ma50 = vol.rolling(50).mean().iloc[-1]

        # --- 相对强度 (RS) 枢纽 ---
        rs_line = close / spy_df
        # 奇点感知：RS线创20日新高 且 股价波动极小(VCP)
        rs_nh_20 = rs_line.iloc[-1] >= rs_line.tail(20).max()
        tightness = (close.tail(10).std() / close.tail(10).mean()) * 100
        
        # IBD RS 评分
        def get_perf(d): return (curr_price - close.iloc[-d]) / close.iloc[-d]
        rs_score = (get_perf(63)*2 + get_perf(126) + get_perf(189) + get_perf(252))

        # --- 战术判定 ---
        signals = []
        base_res = 0
        
        # 1. 奇點先行 (感知 GOOGL/CF)
        if rs_nh_20 and tightness < 1.2:
            signals.append("👁️奇點先行")
            base_res += 3
            
        # 2. 巅峰突破
        high_52w = high.tail(252).max()
        if curr_price >= high_52w * 0.98 and vol.iloc[-1] > vol_ma50:
            signals.append("🚀巔峰突破")
            base_res += 2
            
        # 3. 老龍回頭
        if rs_score > 0.6 and abs(curr_price - ma50)/ma50 < 0.03 and vol.iloc[-1] < vol_ma50 * 0.7:
            signals.append("🐉老龍回頭")
            base_res += 2

        if not signals: return None

        # 风险：ADR 与 止损
        adr = ((high - low) / low).tail(20).mean() * 100
        
        return {
            "Ticker": "", "RS_Score": rs_score, "Signals": signals,
            "Base_Res": base_res, "Price": curr_price, "Tightness": tightness,
            "ADR": adr, "MA50": ma50, "MA200": ma200
        }
    except: return None

# ==========================================
# 3. 期权哨兵 (Polygon 5次/分限速)
# ==========================================
def get_option_nexus(ticker):
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        c_val, p_val, sweep = 0, 0, 0
        for s in snaps:
            v = s.day.volume if s.day else 0
            if v < 100: continue
            oi = s.open_interest if s.open_interest else 1
            if v / oi > 2.5: sweep += 1
            val = v * (s.day.last or 0) * 100
            if s.details.contract_type == 'call': c_val += val
            else: p_val += val
        call_pct = round(c_val / (c_val + p_val + 1) * 100, 1)
        return ("🔥扫货" if sweep >= 2 else "活跃" if call_pct > 60 else "平稳"), call_pct
    except: return "N/A", 50.0

# ==========================================
# 4. 主指挥引擎
# ==========================================
def run_v1000_nexus_command():
    print("🏟️ [V1000] 枢纽系统启动：全战区动态扫描...")
    
    # 1. 环境与名册
    env = yf.download(["SPY", "^VIX"], period="2y", progress=False)['Close']
    spy_df = env['SPY'].dropna(); vix = env['^VIX'].iloc[-1]
    
    try:
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        tickers = list(set(sp500['Symbol'].str.replace('.', '-').tolist() + ["TSLA", "PLTR", "MSTR", "NVDA", "CF", "PR"]))
        sector_map = dict(zip(sp500['Symbol'].str.replace('.', '-'), sp500['GICS Sector']))
    except: tickers = ["NVDA", "TSLA", "CF", "PR"]; sector_map = {}

    # 2. 批量审计
    data = yf.download(tickers, period="2y", group_by='ticker', threads=True, progress=False)
    
    candidates = []
    sector_cluster = {} # 板块集群统计

    print(f"🚀 正在演算枢纽信号 (共 {len(tickers)} 只)...")
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            res = calculate_v1000_nexus(data[t].dropna(), spy_df)
            if res:
                res.update({"Ticker": t, "Sector": sector_map.get(t, "Leaders")})
                candidates.append(res)
                # 统计板块集群
                s = res["Sector"]
                sector_cluster[s] = sector_cluster.get(s, 0) + 1
        except: continue

    # 3. 集群加成与财报过滤
    for c in candidates:
        # 集群加成：同板块信号越多，共振越强
        cluster_bonus = min(sector_cluster.get(c["Sector"], 1) - 1, 3)
        c["Total_Res"] = c["Base_Res"] + cluster_bonus

    # 排序：集群共振 > resonance > RS
    final_seeds = pd.DataFrame(candidates).sort_values(by=["Total_Res", "RS_Score"], ascending=False).head(10)
    
    results = []
    print(f"🔥 执行【V1000 终审】：期权穿透 + 财报护盾...")
    for _, row in final_seeds.iterrows():
        opt_status, call_pct = get_option_nexus(row['Ticker'])
        
        # 财报日期
        try:
            cal = yf.Ticker(row['Ticker']).calendar
            earning_date = cal.iloc[0,0] if not cal.empty else None
            if earning_date:
                days_to_e = (earning_date.date() - datetime.date.today()).days
                earning_str = f"{days_to_e}天后"
                if 0 <= days_to_e <= 7: earning_str = "⚠️财报回避"
            else: earning_str = "N/A"
        except: earning_str = "N/A"

        # 最终评级系统
        rating = "🔥强势"
        if row['Total_Res'] >= 5 and call_pct > 65: rating = "💎SSS 枢纽共振"
        elif row['Total_Res'] >= 4: rating = "👑SS 集群联动"
        if earning_str == "⚠️财报回避": rating = "🚫避雷"

        results.append({
            "Ticker": row['Ticker'], "评级": rating, "共振战法": " + ".join(row['Signals']),
            "板块集群": f"{sector_cluster.get(row['Sector'], 1)}只异动",
            "期权状态": opt_status, "看涨%": f"{call_pct}%",
            "财报窗口": earning_str, "Price": row['Price'],
            "止损参考": round(row['Price'] * (1 - row['ADR']*0.015), 2),
            "Res分": row['Total_Res'], "RS强度": round(row['RS_Score'], 2),
            "板块": row['Sector']
        })
        time.sleep(13) # Polygon 频率保护

    write_to_v1000_sheets(results, vix, spy_df.iloc[-1] > spy_df.rolling(50).mean().iloc[-1])

# ==========================================
# 5. 写入与安全清洗
# ==========================================
def robust_json_clean(val):
    if val is None or pd.isna(val): return ""
    if isinstance(val, (float, np.floating, np.float64)):
        return float(round(val, 3)) if math.isfinite(val) else 0.0
    return str(val)

def write_to_v1000_sheets(results, vix, spy_healthy):
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("us Screener")
        sh.clear()

        weather = "☀️ 激进" if (spy_healthy and vix < 20) else "⛈️ 缩手" if vix > 26 else "☁️ 震荡"
        bj_time = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=8)).strftime('%m-%d %H:%M')
        
        header = [
            ["🏰 V1000 终极枢纽系统", "", "Update:", bj_time],
            ["大盘天气:", weather, "VIX:", round(vix, 2), "SPY:", "健康" if spy_healthy else "弱势"],
            ["枢纽定义:", "集群联动(板块效应), 奇點先行(暗盘), 财报护盾(防炸)"],
            []
        ]
        sh.update(range_name="A1", values=header)

        if results:
            df = pd.DataFrame(results)
            clean_vals = [df.columns.tolist()] + [[robust_json_clean(item) for item in row] for row in df.values.tolist()]
            sh.update(range_name="A5", values=clean_vals)
        print("🎉 V1000 指令下达成功！")
    except Exception as e: print(f"❌ 错误: {e}")

if __name__ == "__main__":
    run_v1000_nexus_command()
