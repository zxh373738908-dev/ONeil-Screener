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
# 🛠️ 筹码分布计算核心 (TradingView 模拟)
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
# 🌍 STEP 1: 获取 A 股名册 (暴力生成版)
# ==========================================
def get_a_share_list():
    print("\n🌍 [STEP 1] 启动【暴力号段生成器】：跳过 API，直接生成 A 股号段...")
    # 覆盖 A 股 99.9% 核心活跃板块
    ranges = [
        (600000, 602000), (603000, 606000), # 沪市主板
        (1, 1400), (2000, 3200),           # 深市主板
        (300000, 301600),                  # 创业板
        (688000, 688950)                   # 科创板
    ]
    codes = []
    for start, end in ranges:
        for i in range(start, end):
            codes.append(f"{i:06d}")
    df = pd.DataFrame(codes, columns=['code'])
    print(f"   -> ✅ 已生成 {len(df)} 个探测种子。")
    return df

# ==========================================
# 🚀 STEP 2: 原装方案 + 筹码确认 (核心扫描)
# ==========================================
def scan_market_via_yfinance(df_list):
    print("\n🚀[STEP 2] 启动【T.U.A.W】战法扫描仪 + 筹码确认...")
    
    tickers = []
    for _, row in df_list.iterrows():
        c = str(row['code'])
        t = f"{c}.SS" if c.startswith('6') else f"{c}.SZ"
        tickers.append(t)
        
    all_results = []
    chunk_size = 500
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"   -> 📥 扫描进度: {i}/{len(tickers)} (全球数据中心通道)...")
        try:
            data = yf.download(chunk, period="1y", auto_adjust=True, threads=True, progress=False)
            if data.empty or 'Close' not in data: continue
            
            # 找到有成交量的有效标的
            valid_tickers = []
            if isinstance(data.columns, pd.MultiIndex):
                valid_tickers = data['Close'].columns.tolist()
            else:
                valid_tickers = [chunk[0]] if not data.empty else []

            for ticker in valid_tickers:
                try:
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
                    
                    # 您的原装判定门槛
                    turnover_1 = price * vols[-1]
                    turnover_5 = np.mean(closes[-5:] * vols[-5:])
                    if turnover_5 < 100_000_000 or price < 5: continue 
                    
                    # 指标计算
                    ma20, h250 = np.mean(closes[-20:]), np.max(highs[-250:])
                    vol_ratio = vols[-1] / np.mean(vols[-50:])
                    
                    # 原装 RS 动量
                    r20, r60, r120 = (price/closes[-21]-1), (price/closes[-61]-1), (price/closes[-121]-1)
                    rs_score = (r20*0.4 + r60*0.3 + r120*0.3) * 100
                    dist_high_pct = ((price - h250) / h250) * 100
                    avg_amp5 = np.mean((highs[-5:] - lows[-5:]) / lows[-5:] * 100)

                    # =========================================
                    # ⚔️ 战法判定逻辑 (还原原版 T.U.A.W.)
                    # =========================================
                    cond_mom = (rs_score > 85) or (r60 * 100 > 30)
                    cond_to = 300_000_000 <= turnover_1 <= 1_500_000_000
                    
                    # 伏击/狙击/突破判定
                    fuse = (-8 <= dist_high_pct <= -1) and (vol_ratio < 1.0) and (avg_amp5 < 5.0) and cond_mom and cond_to
                    sniper = (-8 <= dist_high_pct <= 2) and (vol_ratio > 1.5) and cond_mom and cond_to and (price > ma20)
                    breakout = (price > ma20 and vol_ratio > 1.5 and rs_score > 90)

                    if not (fuse or sniper or breakout): continue
                    
                    type_label = "🔥 狙击触发" if sniper else ("🧨 引信雷达" if fuse else "🚀 趋势突破")
                    
                    # 筹码确认
                    poc, res = get_chip_data(df_t)
                    
                    all_results.append({
                        "Ticker": ticker.split('.')[0],
                        "Name": "Fetching...", # 先占位，最后统一获取
                        "Price": round(price, 2),
                        "Type": type_label,
                        "RS_Score": round(rs_score, 2),
                        "POC(筹码中心)": poc,
                        "上方抛压%": res,
                        "Vol_Ratio": round(vol_ratio, 2),
                        "Dist_High%": f"{round(dist_high_pct, 2)}%",
                        "Turnover(亿)": round(turnover_1 / 100000000, 2)
                    })
                except: continue
        except: continue
                
    return all_results

# ==========================================
# 🛰️ 名称修复补丁 (只针对精选出的几十只标的)
# ==========================================
def repair_stock_names(results):
    print("\n🛰️ [STEP 3] 正在通过 Yahoo 接口修复精选标的的名称...")
    if not results: return results
    
    for item in results:
        try:
            t_obj = yf.Ticker(f"{item['Ticker']}.SS" if item['Ticker'].startswith('6') else f"{item['Ticker']}.SZ")
            # 尝试获取短名称，如果失败则保留代码
            item['Name'] = t_obj.info.get('shortName', item['Ticker'])
        except:
            item['Name'] = item['Ticker']
    return results

# ==========================================
# 📝 STEP 4: 写入作战名单
# ==========================================
def write_sheet(data):
    sheet = get_worksheet()
    if not sheet: return
    sheet.clear()
    if not data:
        sheet.update_acell("A1", "No Signals Today.")
        return
    df = pd.DataFrame(data).sort_values("RS_Score", ascending=False).head(50)
    sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
    
    tz_bj = datetime.timezone(datetime.timedelta(hours=8))
    now = datetime.datetime.now(tz=tz_bj).strftime("%Y-%m-%d %H:%M:%S")
    sheet.update_acell("M1", f"Last Update (BJ): {now}")
    print(f"🎉 全部任务完成！指挥部已收到最新战报。")

# ==========================================
# 主程序
# ==========================================
if __name__ == "__main__":
    print(f"\n{'='*40}\n   A股猎手 V9.0 - 终极整合版\n{'='*40}")
    seeds = get_a_share_list()
    raw_results = scan_market_via_yfinance(seeds)
    final_results = repair_stock_names(raw_results)
    write_sheet(final_results)
