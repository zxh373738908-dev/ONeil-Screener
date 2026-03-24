import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging
import yfinance as yf

# 屏蔽干扰
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def get_worksheet():
    try:
        creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
        client = gspread.authorize(creds)
        doc = client.open_by_url(OUTPUT_SHEET_URL)
        try:
            return doc.worksheet("A-Share Screener")
        except gspread.exceptions.WorksheetNotFound:
            return doc.add_worksheet(title="A-Share Screener", rows=1000, cols=20)
    except Exception as e:
        print(f"❌ Google Sheets 连接失败: {e}")
        return None

# ==========================================
# 🛡️ 核心补丁：筹码分布计算 (方案一算法)
# ==========================================
def get_chip_data(df_ticker, lookback=120):
    try:
        hist = df_ticker.tail(lookback)
        p_min, p_max = hist['Low'].min(), hist['High'].max()
        bins = 40
        price_range = np.linspace(p_min, p_max, bins + 1)
        v_dist = np.zeros(bins)
        for _, row in hist.iterrows():
            idx = np.where((price_range[:-1] >= row['Low']) & (price_range[1:] <= row['High']))[0]
            if len(idx) > 0: v_dist[idx] += row['Volume'] / len(idx)
            else:
                c_idx = np.searchsorted(price_range, row['Close']) - 1
                if 0 <= c_idx < bins: v_dist[c_idx] += row['Volume']
        poc_price = (price_range[np.argmax(v_dist)] + price_range[np.argmax(v_dist)+1]) / 2
        curr_price = df_ticker['Close'].iloc[-1]
        curr_idx = np.searchsorted(price_range, curr_price) - 1
        overhead_vol = np.sum(v_dist[curr_idx:]) if curr_idx < bins else 0
        total_vol = np.sum(v_dist)
        res_ratio = (overhead_vol / total_vol) * 100 if total_vol > 0 else 0
        return round(poc_price, 2), f"{round(res_ratio, 1)}%"
    except:
        return 0, "N/A"

# ==========================================
# 🌍 STEP 1: 获取 A 股名册 (修正后的暴力生成版)
# ==========================================
def get_a_share_list():
    print("\n🌍 [STEP 1] 启动【暴力号段生成器】：跳过 API，直接生成 A 股号段...")
    
    # 修正语法：不再使用 000001 这种数字，改用普通整数
    ranges = [
        (600000, 602000), # 沪市主板 1
        (603000, 606000), # 沪市主板 2
        (1, 1400),        # 深市主板 (修正了 000001 的语法错误)
        (2000, 3200),     # 深市主板/中小板
        (300000, 301600), # 创业板
        (688000, 688900)  # 科创板
    ]
    
    codes = []
    for start, end in ranges:
        for i in range(start, end):
            # 将整数格式化为 6 位字符串，如 1 变为 "000001"
            codes.append(f"{i:06d}")
    
    df = pd.DataFrame(codes, columns=['code'])
    df['name'] = df['code'] # API 挂了，暂时以代码作为名称
    print(f"   -> ✅ 已生成 {len(df)} 个探测种子。")
    return df

# ==========================================
# 🚀 STEP 2: 原装方案 + 筹码确认 (逻辑 0 修改)
# ==========================================
def scan_market_via_yfinance(df_list):
    print("\n🚀[STEP 2] 启动【T.U.A.W】原装战法扫描仪...")
    
    tickers = []
    for _, row in df_list.iterrows():
        c = str(row['code'])
        # 沪市(60, 68)使用 .SS，深市(00, 30)使用 .SZ
        t = f"{c}.SS" if c.startswith('6') else f"{c}.SZ"
        tickers.append(t)
        
    all_results = []
    chunk_size = 500 # 分块下载
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"   -> 📥 扫描进度: {i}/{len(tickers)} (Yahoo Finance 美国通道)...")
        try:
            # yfinance 极其强悍，它会忽略不存在的代码，只返回有效的标的
            data = yf.download(chunk, period="1y", auto_adjust=True, threads=True, progress=False)
            
            if data.empty or 'Close' not in data:
                continue
                
            # 找到本次下载中确实有数据的有效代码
            if isinstance(data.columns, pd.MultiIndex):
                valid_tickers = data['Close'].columns.tolist()
            else:
                valid_tickers = [chunk[0]] if not data.empty else []

            for ticker in valid_tickers:
                try:
                    # 提取单只股票数据
                    if isinstance(data.columns, pd.MultiIndex):
                        df_t = pd.DataFrame({
                            'Open': data['Open'][ticker], 'High': data['High'][ticker],
                            'Low': data['Low'][ticker], 'Close': data['Close'][ticker],
                            'Volume': data['Volume'][ticker]
                        }).dropna()
                    else:
                        df_t = data.dropna()
                    
                    if len(df_t) < 200: continue
                    
                    closes, highs, lows, vols = df_t['Close'].values, df_t['High'].values, df_t['Low'].values, df_t['Volume'].values
                    price = closes[-1]
                    
                    # --- 您的原装判定门槛 ---
                    turnover_1 = price * vols[-1]
                    turnover_5 = np.mean(closes[-5:] * vols[-5:])
                    if turnover_5 < 100_000_000 or price < 5: continue 
                    
                    # 指标演算
                    ma20, ma50 = np.mean(closes[-20:]), np.mean(closes[-50:])
                    ma150, ma200 = np.mean(closes[-150:]), np.mean(closes[-200:])
                    h250 = np.max(highs[-250:])
                    vol_ratio = vols[-1] / np.mean(vols[-50:])
                    
                    # RSI
                    deltas = np.diff(closes[-30:])
                    gain = np.where(deltas > 0, deltas, 0)
                    loss = np.where(deltas < 0, -deltas, 0)
                    avg_gain = pd.Series(gain).ewm(com=13, adjust=False).mean().iloc[-1]
                    avg_loss = pd.Series(loss).ewm(com=13, adjust=False).mean().iloc[-1]
                    rsi = 100 if avg_loss == 0 else 100 - (100 / (1 + (avg_gain / avg_loss)))
                    
                    # 原装 RS 动量逻辑
                    r20, r60, r120 = (price/closes[-21]-1), (price/closes[-61]-1), (price/closes[-121]-1)
                    rs_score = (r20*0.4 + r60*0.3 + r120*0.3) * 100
                    dist_high_pct = ((price - h250) / h250) * 100
                    avg_amp5 = np.mean((highs[-5:] - lows[-5:]) / lows[-5:] * 100)

                    # =========================================
                    # ⚔️ 判定逻辑 (原封不动还原您的战法)
                    # =========================================
                    cond_mom = (rs_score > 85) or (r60 * 100 > 30)
                    cond_to = 300_000_000 <= turnover_1 <= 1_500_000_000
                    
                    fuse = (-8 <= dist_high_pct <= -1) and (vol_ratio < 1.0) and (avg_amp5 < 5.0) and cond_mom and cond_to
                    sniper = (-8 <= dist_high_pct <= 2) and (vol_ratio > 1.5) and cond_mom and cond_to and (price > ma20)
                    breakout = (price > ma20 and price > ma50 and ma50 > ma150 and ma150 > ma200 and vol_ratio > 1.5 and rsi > 60)
                    ambush = (abs(price - ma20) / ma20 < 0.03 and vol_ratio < 1.1 and ma50 > ma150 and ma150 > ma200)

                    if not (fuse or sniper or breakout or ambush): continue
                    
                    type_label = "🔥 狙击触发" if sniper else ("🧨 引信雷达" if fuse else ("🚀 趋势突破" if breakout else "🧘 均线伏击"))
                    
                    # 筹码确认 (方案一算法)
                    poc, res = get_chip_data(df_t)
                    
                    all_results.append({
                        "Ticker": ticker.split('.')[0],
                        "Name": ticker.split('.')[0], # 以代码代替名称
                        "Price": round(price, 2),
                        "Type": type_label,
                        "RS_Score": round(rs_score, 2),
                        "POC(筹码中心)": poc,
                        "上方抛压%": res,
                        "RSI": round(rsi, 2),
                        "Vol_Ratio": round(vol_ratio, 2),
                        "Dist_High%": f"{round(dist_high_pct, 2)}%",
                        "Turnover(亿)": round(turnover_1 / 100000000, 2)
                    })
                except: continue
        except Exception as e:
            print(f"   -> ⚠️ 块扫描异常: {e}")
            continue
                
    return all_results

# ==========================================
# 📝 STEP 3: 写入
# ==========================================
def write_sheet(data):
    sheet = get_worksheet()
    if not sheet: return
    sheet.clear()
    if not data:
        sheet.update_acell("A1", "No Signals. (暴力扫描模式)")
        return
    df = pd.DataFrame(data).sort_values("RS_Score", ascending=False).head(50)
    sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
    
    tz_bj = datetime.timezone(datetime.timedelta(hours=8))
    now = datetime.datetime.now(tz=tz_bj).strftime("%Y-%m-%d %H:%M:%S")
    sheet.update_acell("M1", f"Last Update (BJ): {now}")
    print(f"🎉 任务完成！")

if __name__ == "__main__":
    print(f"\n{'='*40}\n   A股猎手 V8.9 - 暴力穿透修复版\n{'='*40}")
    shares = get_a_share_list()
    results = scan_market_via_yfinance(shares)
    write_sheet(results)
