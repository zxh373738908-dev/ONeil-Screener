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
# 2. V20 泰坦指挥部算法库
# ==========================================
def calculate_v20_engine(df, spy_df, dxy_trend):
    close = df['Close']
    vol = df['Volume']
    
    # 1. 相对强度 & RS线先行
    rs_line = close / spy_df['Close']
    rs_nh = rs_line.iloc[-1] >= rs_line.tail(20).max()
    rs_val = (close.iloc[-1]/close.iloc[-63]) / (spy_df['Close'].iloc[-1]/spy_df['Close'].iloc[-63])
    
    # 2. 均线与缩量 (V18核心)
    ma50 = close.rolling(50).mean().iloc[-1]
    vol_ma50 = vol.rolling(50).mean().iloc[-1]
    is_vol_exhausted = vol.iloc[-1] < (vol_ma50 * 0.65) # 缩量至65%以下
    
    # 3. ATR 移动止盈 (V16回归)
    tr = pd.concat([(df['High']-df['Low']), (df['High']-df['Close'].shift(1)).abs(), (df['Low']-df['Close'].shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean().iloc[-1]
    trailing_stop = close.iloc[-1] - (2.5 * atr)
    
    # 4. VCP & 口袋突破
    vcp = df['Close'].tail(10).std() / df['Close'].tail(30).std()
    down_vol = df[df['Close'] < df['Open']]['Volume'].tail(10).max()
    is_pocket = (close.iloc[-1] > df['Open'].iloc[-1]) and (vol.iloc[-1] > (down_vol if not pd.isna(down_vol) else 0))
    
    # 5. 乖离率
    extension = (close.iloc[-1] - ma50) / ma50

    return {
        "rs_val": rs_val, "rs_nh": rs_nh, "ma50": ma50,
        "is_vol_exhausted": is_vol_exhausted, "vcp": vcp,
        "is_pocket": is_pocket, "trailing_stop": trailing_stop,
        "extension": extension, "vol_ratio": vol.iloc[-1]/vol_ma50
    }

# ==========================================
# 3. 主扫描流程
# ==========================================
def run_v20_titan():
    print("🏟️ [1/3] 泰坦指挥部：扫描全球宏观因子...")
    macro = yf.download(["DX-Y.NYB", "^TNX", "SPY", "^VIX"], period="5d", progress=False)['Close']
    dxy_trend = "DOWN" if macro["DX-Y.NYB"].iloc[-1] < macro["DX-Y.NYB"].iloc[0] else "UP"
    vix_val = macro["^VIX"].iloc[-1]
    
    # 获取名册
    headers = {'User-Agent': 'Mozilla/5.0'}
    tickers = list(set(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].tolist() + ["PR", "CF", "NTR", "FANG"]))
    
    data = yf.download(tickers + ["SPY"], period="2y", group_by='ticker', threads=True, progress=False)
    spy_df = data["SPY"].dropna()

    candidates = []
    print(f"🚀 [2/3] 执行【龙心战术 + 宏观共振】闭环演算...")
    
    for t in tickers:
        try:
            if t not in data.columns.levels[0]: continue
            df = data[t].dropna()
            if len(df) < 200: continue
            
            v20 = calculate_v20_engine(df, spy_df, dxy_trend)
            close = float(df['Close'].iloc[-1])
            
            # --- 指令判定系统 ---
            action = "💤 观望"
            priority = 99
            
            # 1. 🐉 老龙回头 (优先权1)
            near_ma50 = abs(close - v20['ma50'])/v20['ma50'] < 0.03
            if v20['rs_val'] > 1.15 and v20['is_vol_exhausted'] and near_ma50:
                action, priority = "🐉 老龙回头(低吸)", 1
            
            # 2. 🎯 口袋突破 (优先权2)
            elif v20['is_pocket'] and v20['vcp'] < 0.8 and v20['rs_val'] > 1.1:
                action, priority = "🎯 口袋突破(加仓)", 2
            
            # 3. ⚠️ 乖离警报 (优先权3)
            elif v20['extension'] > 0.16:
                action, priority = "⚠️ 乖离过大(避开)", 3
            
            # 4. 👑 趋势领袖
            elif v20['rs_val'] > 1.2 and v20['rs_nh']:
                action, priority = "👑 领袖持仓(锁仓)", 4

            if action != "💤 观望":
                candidates.append({
                    "Ticker": t, "Action": action, "Priority": priority,
                    "RS评级": round(v20['rs_val']*80, 1), "Price": round(close, 2),
                    "ATR移动止盈": round(v20['trailing_stop'], 2),
                    "宏观": "✅顺风" if (dxy_trend=="DOWN") else "-",
                    "RS线": "🌟先行" if v20['rs_nh'] else "跟随"
                })
        except: continue

    # 排序：按优先级（低吸和加仓排最前）
    seeds = sorted(candidates, key=lambda x: x['Priority'])[:5]

    print(f"🔥 [3/3] 期权哨兵核验 (Polygon 精确分配)...")
    results = []
    for item in seeds:
        opt_score, opt_total = get_sentiment_v20(item['Ticker'])
        item.update({"期权分": opt_score, "期权规模": f"${round(opt_total/1e6, 2)}M"})
        results.append(item)
        time.sleep(13)

    output_v20(results, dxy_trend, vix_val)

def get_sentiment_v20(ticker):
    try:
        snaps = client_poly.get_snapshot_options_chain(ticker)
        bull, total = 0, 0
        for s in snaps:
            val = s.day.volume * (s.day.last or 0) * 100
            if val > 30000:
                total += val
                if s.details.contract_type == 'call': bull += val
        return (round((bull/total)*100, 1), total) if total > 0 else (50, 0)
    except: return (50, 0)

def output_v20(res, dxy, vix):
    try:
        creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SHEET_ID).worksheet("Screener")
        sh.clear()
        
        header = [
            ["🏟️ [V20 泰坦指挥部 - Titan Command]", "", "Update:", datetime.datetime.now().strftime('%m-%d %H:%M')],
            ["美元趋势:", dxy, "VIX指数:", round(vix, 2)],
            ["操作建议:", "寻找 ✅顺风 + 🐉老龙回头 的极品组合"],
            ["", "", "", ""],
            ["=== V20 终极作战指令 (宏观环境 + 战术节奏 + 移动止盈) ==="]
        ]
        sh.update(values=header, range_name="A1")
        if res:
            df = pd.DataFrame(res)
            cols = ["Ticker", "Action", "RS评级", "宏观", "RS线", "Price", "ATR移动止盈", "期权分", "期权规模"]
            df = df[cols]
            sh.update(values=[df.columns.tolist()] + df.values.tolist(), range_name="A5")
        print("🎉 V20 泰坦指令已装填完毕！")
    except Exception as e: print(f"❌ 失败: {e}")

if __name__ == "__main__":
    run_v20_titan()
