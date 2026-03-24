import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging
import yfinance as yf

# 屏蔽所有不必要的日志
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
# 🛠️ 筹码分布计算核心 (方案一：本地模拟算法)
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
            if len(idx) > 0:
                v_dist[idx] += row['Volume'] / len(idx)
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
# 🌍 STEP 1: 获取 A 股名册 (暴力生成版)
# ==========================================
def get_a_share_list():
    print("\n🌍 [STEP 1] 启动【号段生成器】：跳过 API，直接覆盖全市场核心号段...")
    ranges = [
        (600000, 602000), (603000, 606000), # 沪市主板
        (1, 1500), (2000, 3200),           # 深市主板
        (300000, 301650),                  # 创业板
        (688000, 688990)                   # 科创板
    ]
    codes = [f"{i:06d}" for start, end in ranges for i in range(start, end)]
    return pd.DataFrame(codes, columns=['code'])

# ==========================================
# 🚀 STEP 2: 原装方案 + 筹码确认 (全速扫描)
# ==========================================
def scan_market_via_yfinance(df_list):
    print("\n🚀[STEP 2] 启动【T.U.A.W】战法扫描仪式 (正在穿透铁幕)...")
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_list['code']]
    
    all_results = []
    chunk_size = 500  # 保持 500 的块大小以平衡速度和稳定性
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"   -> 📥 扫描进度: {i}/{len(tickers)} (全球数据通道传输中)...")
        try:
            data = yf.download(chunk, period="1y", auto_adjust=True, threads=True, progress=False)
            if data.empty or 'Close' not in data: continue
            
            valid_tickers = data['Close'].columns.tolist() if isinstance(data.columns, pd.MultiIndex) else ([chunk[0]] if not data.empty else [])

            for t in valid_tickers:
                try:
                    # 数据切片
                    if isinstance(data.columns, pd.MultiIndex):
                        df_t = pd.DataFrame({
                            'Open': data['Open'][t], 'High': data['High'][t],
                            'Low': data['Low'][t], 'Close': data['Close'][t],
                            'Volume': data['Volume'][t]
                        }).dropna()
                    else:
                        df_t = data.dropna()
                    
                    if len(df_t) < 200: continue
                    
                    closes, highs, lows, vols = df_t['Close'].values, df_t['High'].values, df_t['Low'].values, df_t['Volume'].values
                    price = closes[-1]
                    
                    # 您的原装逻辑门槛 (Turnover 调整为 1.5亿起，过滤僵尸股)
                    turnover_1 = price * vols[-1]
                    if turnover_1 < 150_000_000 or price < 5: continue 
                    
                    vol_ratio = vols[-1] / np.mean(vols[-50:])
                    h250 = np.max(highs[-250:])
                    r20, r60, r120 = (price/closes[-21]-1), (price/closes[-61]-1), (price/closes[-121]-1)
                    rs_score = (r20*0.4 + r60*0.3 + r120*0.3) * 100
                    dist_high_pct = ((price - h250) / h250) * 100
                    amp5 = np.mean((highs[-5:] - lows[-5:]) / lows[-5:] * 100)

                    # =========================================
                    # ⚔️ 战法判定逻辑 (还原原版 T.U.A.W.)
                    # =========================================
                    cond_mom = (rs_score > 85) or (r60 * 100 > 30)
                    # 判定 1: 引信雷达 (缩量伏击)
                    fuse = (-8 <= dist_high_pct <= -1) and (vol_ratio < 1.0) and (amp5 < 5.0) and cond_mom
                    # 判定 2: 狙击触发 (突破点火)
                    sniper = (-8 <= dist_high_pct <= 2) and (vol_ratio > 1.5) and cond_mom and (price > np.mean(closes[-20:]))
                    
                    if not (fuse or sniper): continue
                    
                    # 计算筹码峰确认数据
                    poc, res = get_chip_data(df_t)
                    
                    all_results.append({
                        "Ticker": t.split('.')[0],
                        "Type": "🔥 狙击" if sniper else "🧨 伏击",
                        "Price": round(price, 2),
                        "RS_Score": round(rs_score, 2),
                        "POC(筹码中心)": poc,
                        "上方抛压%": res,
                        "量比": round(vol_ratio, 2),
                        "距高点%": f"{round(dist_high_pct, 2)}%",
                        "成交额(亿)": round(turnover_1 / 100000000, 2)
                    })
                except: continue
        except: continue
                
    return all_results

# ==========================================
# 📝 STEP 3: 极速写入
# ==========================================
def write_sheet(data):
    sheet = get_worksheet()
    if not sheet: return
    sheet.clear()
    if not data:
        sheet.update_acell("A1", "No Signal Today. (Extreme Speed Mode)")
        return
    
    df = pd.DataFrame(data).sort_values("RS_Score", ascending=False).head(50)
    sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
    
    # 记录更新时间
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    sheet.update_acell("K1", f"V9.2 Extreme Speed Last Update (BJ): {now}")
    print(f"🎉 任务秒杀完成！数据已同步。")

# ==========================================
# 主流程
# ==========================================
if __name__ == "__main__":
    print(f"\n{'='*50}\n   A股猎手 V9.2 - 极速战镰版\n{'='*50}")
    seeds = get_a_share_list()
    results = scan_market_via_yfinance(seeds)
    write_sheet(results)
