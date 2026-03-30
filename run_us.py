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
# 2. V50 核心算法库 (增加鲁棒性)
# ==========================================
def calculate_v50_metrics(df, spy_df):
    try:
        close = df['Close']
        vol = df['Volume']
        
        # 1. 垂直加速度 (Verticality)
        rs_line = close / spy_df['Close']
        # 增加 fillna(0) 防止 inf
        slope_now = (rs_line.iloc[-1] - rs_line.iloc[-6]) / (rs_line.iloc[-6] if rs_line.iloc[-6] != 0 else 0.001)
        slope_prev = (rs_line.iloc[-7] - rs_line.iloc[-12]) / (rs_line.iloc[-12] if rs_line.iloc[-12] != 0 else 0.001)
        acceleration = slope_now - slope_prev
        
        # 2. 紧致度 (Tightness)
        t_val = close.tail(10).std() / close.tail(10).mean()
        tightness = t_val if not np.isnan(t_val) else 0.05
        
        # 3. U/D 量能比
        up_v = vol[df['Close'] > df['Open']].tail(40).sum()
        dn_v = vol[df['Close'] < df['Open']].tail(40).sum()
        ud_ratio = up_v / (dn_v if dn_v > 0 else 1)
        
        # 4. ATR 动态移动止盈
        high_low = df['High'] - df['Low']
        high_pc = (df['High'] - close.shift(1)).abs()
        low_pc = (df['Low'] - close.shift(1)).abs()
        tr = pd.concat([high_low, high_pc, low_pc], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        trailing_stop = close.iloc[-1] - (2.5 * (atr if not np.isnan(atr) else close.iloc[-1]*0.03))
        
        # 5. 筹码中心 POC
        counts, bin_edges = np.histogram(close.tail(120), bins=50, weights=vol.tail(120))
        poc = (bin_edges[np.argmax(counts)] + bin_edges[np.argmax(counts)+1]) / 2
        
        # 6. 综合强度
        rs_raw = (close.iloc[-1] / close.iloc[-63]) / (spy_df['Close'].iloc[-1] / spy_df['Close'].iloc[-63])
        score = rs_raw * ud_ratio * (1 / (tightness * 100 if tightness > 0 else 1))
        
        return {
            "score": score, "acceleration": acceleration, "tightness": round(tightness*100, 3),
            "ud_ratio": round(ud_ratio, 2), "trailing_stop": round(trailing_stop, 2),
            "poc": round(poc, 2), "rs_raw": round(rs_raw, 2),
            "dist_high": (close.iloc[-1] - df['High'].max()) / df['High'].max()
        }
    except Exception:
        return None

# ==========================================
# 3. 选股扫描引擎
# ==========================================
def run_v50_citadel():
    print("📡 [1/3] 天基指挥部启动：正在执行全球宏观共振与加速度分析...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        sp500_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]
        raw_tickers = sp500_df['Symbol'].tolist()
        # 修复 Yahoo Finance 的符号格式 (. 变 -)
        tickers = [t.replace('.', '-') for t in raw_tickers]
        tickers = list(set(tickers + ["PR", "CF", "NTR", "FANG", "NVDA", "GOOGL"]))
        ticker_to_sector = dict(zip(tickers, sp500_df['GICS Sector'].tolist() + ["Energy"]*6))
    except:
        print("❌ 获取名册失败，请检查网络。")
        return

    # 宏观环境
    env = yf.download(["DX-Y.NYB", "^VIX", "SPY"], period="5d", progress=False)['Close']
    vix = env["^VIX"].iloc[-1] if not env["^VIX"].empty else 20
    
    data = yf.download(tickers + ["SPY"], period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"].dropna()

    pre_candidates = []
    sector_cluster = {}
    ma50_count = 0

    print(f"🚀 [2/3] 正在演算 {len(tickers)} 个目标的【垂直加速度】...")
    
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 200: continue
            
            close = df['Close'].iloc[-1]
            if close < df['Close'].tail(200).mean(): continue
            if close > df['Close'].tail(50).mean(): ma50_count += 1
            
            v50 = calculate_v50_metrics(df, spy_df)
            if not v50: continue
            
            is_explosion = (v50['dist_high'] >= -0.05) and (v50['acceleration'] > 0)
            is_dip = (0 <= (close - v50['poc'])/v50['poc'] <= 0.06) and (v50['rs_raw'] > 1.1)
            
            if is_explosion or is_dip:
                sec = ticker_to_sector.get(t, "Other")
                sector_cluster[sec] = sector_cluster.get(sec, 0) + 1
                pre_candidates.append({
                    "Ticker": t, "Action": "🚀垂直爆破" if is_explosion else "🐉支撑回踩",
                    "Sector": sec, "总分": v50['score'], "加速": "仰攻📈" if v50['acceleration'] > 0 else "走平",
                    "紧致度": v50['tightness'], "U/D比": v50['ud_ratio'], "移动止盈": v50['trailing_stop'],
                    "POC": v50['poc'], "Price": round(close, 2)
                })
        except: continue

    breadth = ma50_count / len(tickers) if len(tickers) > 0 else 0
    weather = "☀️ 极佳" if (breadth > 0.6 and vix < 22) else "⛈️ 风险" if (breadth < 0.4 or vix > 28) else "☁️ 震荡"

    for c in pre_candidates: c['共振数'] = sector_cluster.get(c['Sector'], 0)
    seeds = sorted(pre_candidates, key=lambda x: (x['共振数'] >= 3, x['总分']), reverse=True)[:5]

    print(f"🔥 [3/3] 调动期权雷达执行最终核验...")
    results = []
    for item in seeds:
        opt_score, opt_desc = get_sentiment_v50(item['Ticker'])
        try:
            t_obj = yf.Ticker(item['Ticker'])
            # 强化财报抓取鲁棒性
            cal = t_obj.calendar
            if cal is not None and not cal.empty:
                days_to_e = (cal.iloc[0, 0].date() - datetime.date.today()).days
                eb_str = "⚠️临近" if 0 <= days_to_e <= 7 else f"{days_to_e}天后"
            else:
                eb_str = "未知"
        except: eb_str = "未知"
        
        item.update({
            "评级": "💎SSS+" if (opt_score > 65 and item['共振数'] >= 3) else "🔥强势",
            "财报": eb_str, "期权": f"{opt_score}% Call", "规模": opt_desc, "共振": f"{item['共振数']}家"
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
        return round((bull/total)*100, 1) if total > 0 else 50, f"${round(total/1e6, 2)}M"
    except: return 50, "N/A"

def output_v50_to_sheets(res, weather, breadth, vix):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        header = [
            ["🏰 [V50 天基指挥部终极版]", "", "Update:", datetime.datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["环境天气:", weather, "大盘宽度:", f"{round(breadth*100, 1)}%", "VIX:", round(vix, 2)],
            ["操作指令:", "移动止盈是生命线。不破该位，死抱盈利！"],
            ["", "", "", ""]
        ]
        sh.update(values=header, range_name="A1")
        if res:
            df = pd.DataFrame(res)
            # --- 关键修复：清洗所有 inf 和 NaN ---
            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.fillna(0) # 将所有无法解析的数值转为 0
            
            cols = ["Ticker", "评级", "Action", "共振", "加速", "移动止盈", "POC", "财报", "Price", "期权", "规模", "紧致度"]
            df = df[cols]
            sh.update(values=[df.columns.tolist()] + df.values.tolist(), range_name="A5")
        print("🎉 V50 天基任务执行完毕，情报已送达！")
    except Exception as e:
        print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    run_v50_citadel()
