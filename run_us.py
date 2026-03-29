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
# 1. 核心配置
# ==========================================
POLYGON_API_KEY = "您的_API_KEY"
client_poly = RESTClient(POLYGON_API_KEY)
SHEET_ID = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"
creds_file = "credentials.json"

# ==========================================
# 2. 核心量化算法库
# ==========================================
def calculate_atr(df, window=14):
    high_low = df['High'] - df['Low']
    high_pc = (df['High'] - df['Close'].shift(1)).abs()
    low_pc = (df['Low'] - df['Close'].shift(1)).abs()
    tr = pd.concat([high_low, high_pc, low_pc], axis=1).max(axis=1)
    return tr.rolling(window=window).mean().iloc[-1]

def run_v16_nexus():
    print("🌐 [1/3] 枢纽系统启动：正在同步全球宏观因子 (DXY/TNX)...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 宏观环境扫描
    macro_data = yf.download(["DX-Y.NYB", "^TNX", "SPY", "^VIX"], period="5d", progress=False)['Close']
    dxy_trend = "DOWN" if macro_data["DX-Y.NYB"].iloc[-1] < macro_data["DX-Y.NYB"].iloc[0] else "UP"
    vix_val = macro_data["^VIX"].iloc[-1]
    
    # 获取名册
    sp500_data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
    tickers = list(set(sp500_data['Symbol'].tolist() + ["PR", "CF", "NTR", "FANG", "MSTR"]))
    
    # 批量数据下载 (2年历史以支撑 ATR 和 POC)
    data = yf.download(tickers + ["SPY"], period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"].dropna()

    print(f"🚀 [2/3] 执行【宏观加权 + ATR动态追踪】演算...")
    
    candidates = []
    sector_signals = {}
    
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 150: continue
            
            close = float(df['Close'].iloc[-1])
            sector = sp500_data[sp500_data['Symbol'] == t]['GICS Sector'].values[0] if t in sp500_data['Symbol'].values else "Energy/Materials"
            
            # --- V16 核心演算 ---
            # 1. RS线先行 (宙斯盾核心)
            rs_line = df['Close'] / spy_df['Close']
            is_rs_nh = rs_line.iloc[-1] >= rs_line.tail(20).max()
            
            # 2. 宏观共振 (Nexus 核心)
            # 如果是资源类股票且美元下跌，给予额外加权
            macro_boost = True if (sector in ["Energy", "Materials"] and dxy_trend == "DOWN") else False
            
            # 3. ATR 移动止盈位
            atr = calculate_atr(df)
            trailing_stop = close - (2.5 * atr)
            
            # 4. 筹码中心 POC
            counts, bin_edges = np.histogram(df['Close'].tail(120), bins=50, weights=df['Volume'].tail(120))
            poc = (bin_edges[np.argmax(counts)] + bin_edges[np.argmax(counts)+1]) / 2
            
            # 判定形态
            dist_high = (close - df['High'].max()) / df['High'].max()
            is_valid = (dist_high >= -0.05) and (close > df['Close'].tail(50).mean())
            
            if is_valid and (df['Close'].iloc[-1] / df['Close'].iloc[-63] > 1.1):
                sector_signals[sector] = sector_signals.get(sector, 0) + 1
                candidates.append({
                    "Ticker": t,
                    "Sector": sector,
                    "RS线先行": "🌟YES" if is_rs_nh else "NO",
                    "宏观加成": "✅DXY顺风" if macro_boost else "-",
                    "Price": round(close, 2),
                    "POC支撑": round(poc, 2),
                    "ATR移动止盈": round(trailing_stop, 2),
                    "RS_Score": round((close/df['Close'].iloc[-63])/(spy_df['Close'].iloc[-1]/spy_df['Close'].iloc[-63]), 2)
                })
        except: continue

    # 排序优先级：RS线先行 > 宏观加成 > RS评分
    elite_list = sorted(candidates, key=lambda x: (x['RS线先行']=="🌟YES", x['宏观加成']=="✅DXY顺风"), reverse=True)[:5]

    print(f"🔥 [3/3] 期权哨兵 + 财报护盾深度扫描...")
    results = []
    for item in elite_list:
        # 期权体检
        opt_score, opt_flow = get_sentiment_v16(item['Ticker'])
        # 财报护盾
        try:
            cal = yf.Ticker(item['Ticker']).calendar
            days_to_e = (cal.iloc[0, 0].date() - datetime.date.today()).days if not cal.empty else 99
        except: days_to_e = 99
        
        cluster = sector_signals.get(item['Sector'], 0)
        
        # 评级
        final_rank = "💎SSS+" if (item['RS线先行']=="🌟YES" and item['宏观加成']=="✅DXY顺风" and opt_score > 65) else "🔥强势"
        if days_to_e <= 7: final_rank = "🚫避开财报"

        item.update({
            "最终评级": final_rank,
            "集群共振": f"{cluster}家",
            "财报窗口": f"{days_to_e}天" if days_to_e < 30 else "安全",
            "期权分": opt_score,
            "期权大单": opt_flow
        })
        results.append(item)
        time.sleep(13)

    output_v16(results, dxy_trend, vix_val)

def get_sentiment_v16(ticker):
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        bull, total = 0, 0
        for s in snaps:
            val = s.day.volume * (s.day.last or 0) * 100
            if val > 30000:
                total += val
                if s.details.contract_type == 'call': bull += val
        return round((bull/total)*100, 1) if total > 0 else 50, f"${round(total/1e6, 2)}M"
    except: return 50, "N/A"

def output_v16(res, dxy, vix):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        header = [
            ["🌐 [V16.0 枢纽指挥中心]", f"美元趋势: {dxy}", "VIX 指数:", round(vix, 2)],
            ["操作环境:", "🟢 极佳" if (dxy=="DOWN" and vix<20) else "🟡 谨慎" if vix<25 else "🔴 风险"],
            ["", "", "", ""],
            ["=== 枢纽级锁定 (宏观共振 + RS先行 + 移动止盈) ==="]
        ]
        sh.update(values=header, range_name="A1")
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "最终评级", "RS线先行", "宏观加成", "ATR移动止盈", "POC支撑", "财报窗口", "集群共振", "Price", "期权分", "期权大单"]
            df = df[cols]
            sh.update(values=[df.columns.tolist()] + df.values.tolist(), range_name="A5")
        print("🎉 V16.0 枢纽指令已下达！")
    except Exception as e: print(f"❌ 失败: {e}")

if __name__ == "__main__":
    run_v16_nexus()
