import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests
import yfinance as yf
import akshare as ak

# 基础屏蔽
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
# 🛡️ 核心补丁：筹码分布计算 (只做确认)
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
        res_ratio = (overhead_vol / np.sum(v_dist)) * 100 if np.sum(v_dist)>0 else 0
        return round(poc_price, 2), f"{round(res_ratio, 1)}%"
    except:
        return 0, "N/A"

# ==========================================
# 🌍 STEP 1: 获取 A 股名册 (最强 API 穿透版)
# ==========================================
def get_a_share_list():
    print("\n🌍 [STEP 1] 启动【底层穿透】：直接从数据中心获取名册...")
    
    # 东财 API 终极伪装
    url = "https://19.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1", "pz": "5500", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f3",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
        "fields": "f12,f14"  # f12:代码, f14:名称
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/center/gridlist.html"
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        data = r.json()
        stocks = data.get('data', {}).get('diff', [])
        if stocks:
            df = pd.DataFrame(stocks)
            df.columns = ['code', 'name']
            print(f"   -> ✅ API 穿透成功！获取到 {len(df)} 只初始标的。")
            
            # 清洗
            df['code'] = df['code'].astype(str)
            df = df[df['code'].str.match(r'^(60|68|00|30)')]
            df = df[~df['name'].astype(str).str.contains('ST|退', case=False)]
            print(f"   -> ✅ 洗盘完毕！锁定 {len(df)} 只优质 A 股进入演算。")
            return df
    except Exception as e:
        print(f"   -> ⚠️ API 穿透受阻: {e}，尝试备用 akshare 接口...")
    
    # 备用方案 (akshare)
    try:
        df = ak.stock_zh_a_spot_em()
        df = df.rename(columns={'代码': 'code', '名称': 'name'})
        return df[['code', 'name']]
    except:
        print("❌ 致命错误：所有数据源均无法访问。")
        return pd.DataFrame()

# ==========================================
# 🚀 STEP 2: 原装方案扫描仪 (逻辑 0 修改)
# ==========================================
def scan_market_via_yfinance(df_list):
    print("\n🚀[STEP 2] 启动【T.U.A.W】原装战法演算 + 筹码确认...")
    
    tickers = []
    ticker_to_name = {}
    for _, row in df_list.iterrows():
        c = str(row['code'])
        t = f"{c}.SS" if c.startswith('6') else f"{c}.SZ"
        tickers.append(t)
        ticker_to_name[t] = row['name']
        
    all_results = []
    chunk_size = 500  # 小分块更稳定
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"   -> 📥 正在演算区块 {i//chunk_size + 1}/{int(len(tickers)/chunk_size)+1}...")
        
        try:
            # yfinance 在海外运行极其丝滑，不会被封
            data = yf.download(chunk, period="1y", auto_adjust=True, threads=True, progress=False)
        except:
            continue
        
        for ticker in chunk:
            try:
                if len(chunk) > 1:
                    df_ticker = pd.DataFrame({
                        'Open': data['Open'][ticker], 'High': data['High'][ticker],
                        'Low': data['Low'][ticker], 'Close': data['Close'][ticker],
                        'Volume': data['Volume'][ticker]
                    }).dropna()
                else:
                    df_ticker = data.dropna()
                
                if len(df_ticker) < 200: continue
                
                closes, highs, lows, vols = df_ticker['Close'].values, df_ticker['High'].values, df_ticker['Low'].values, df_ticker['Volume'].values
                price = closes[-1]
                
                # 基础门槛
                turnover_1 = price * vols[-1]
                turnover_5 = np.mean(closes[-5:] * vols[-5:])
                if turnover_5 < 100_000_000 or price < 5: continue 
                
                # 指标计算
                ma20 = np.mean(closes[-20:])
                ma50, ma150, ma200 = np.mean(closes[-50:]), np.mean(closes[-150:]), np.mean(closes[-200:])
                h250 = np.max(highs[-250:])
                vol_ratio = vols[-1] / np.mean(vols[-50:])
                
                # RSI
                deltas = np.diff(closes[-30:])
                gain = np.where(deltas > 0, deltas, 0)
                loss = np.where(deltas < 0, -deltas, 0)
                avg_gain = pd.Series(gain).ewm(com=13, adjust=False).mean().iloc[-1]
                avg_loss = pd.Series(loss).ewm(com=13, adjust=False).mean().iloc[-1]
                rsi = 100 if avg_loss == 0 else 100 - (100 / (1 + (avg_gain / avg_loss)))
                
                # 动量 RS
                r20, r60, r120 = (price/closes[-21]-1), (price/closes[-61]-1), (price/closes[-121]-1)
                rs_score = (r20*0.4 + r60*0.3 + r120*0.3) * 100
                dist_high_pct = ((price - h250) / h250) * 100
                avg_amp5 = np.mean((highs[-5:] - lows[-5:]) / lows[-5:] * 100)

                # =========================================
                # ⚔️ 核心逻辑判定 (完全保留原版)
                # =========================================
                cond_momentum = (rs_score > 85) or (r60 * 100 > 30)
                cond_turnover = 300_000_000 <= turnover_1 <= 1_500_000_000
                
                fuse_radar = (-8 <= dist_high_pct <= -1) and (vol_ratio < 1.0) and (avg_amp5 < 5.0) and cond_momentum and cond_turnover
                trigger_sniper = (-8 <= dist_high_pct <= 2) and (vol_ratio > 1.5) and cond_momentum and cond_turnover and (price > ma20)
                breakout = (price > ma20 and price > ma50 and ma50 > ma150 and ma150 > ma200 and vol_ratio > 1.5 and rsi > 60)
                ambush = (abs(price - ma20) / ma20 < 0.03 and vol_ratio < 1.1 and ma50 > ma150 and ma150 > ma200)

                if not (fuse_radar or trigger_sniper or breakout or ambush): continue
                
                type_label = "🔥 狙击触发" if trigger_sniper else ("🧨 引信雷达" if fuse_radar else ("🚀 趋势突破" if breakout else "🧘 均线伏击"))
                
                # 筹码确认
                poc_price, overhead_res = get_chip_data(df_ticker)
                
                all_results.append({
                    "Ticker": ticker.split('.')[0],
                    "Name": ticker_to_name[ticker],
                    "Price": round(price, 2),
                    "Type": type_label,
                    "RS_Score": round(rs_score, 2),
                    "POC(筹码中心)": poc_price,
                    "上方抛压%": overhead_res,
                    "RSI": round(rsi, 2),
                    "Vol_Ratio": round(vol_ratio, 2),
                    "Dist_High%": f"{round(dist_high_pct, 2)}%",
                    "Turnover(亿)": round(turnover_1 / 100000000, 2)
                })
            except: continue
                
    return all_results

# ==========================================
# 📝 STEP 3: 写入表格
# ==========================================
def write_sheet(data):
    sheet = get_worksheet()
    if not sheet: return
    sheet.clear()
    if not data:
        sheet.update_acell("A1", "今日无动量标的。")
        return
    df = pd.DataFrame(data).sort_values("RS_Score", ascending=False).head(60)
    sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
    
    tz_bj = datetime.timezone(datetime.timedelta(hours=8))
    now = datetime.datetime.now(tz=tz_bj).strftime("%Y-%m-%d %H:%M:%S")
    sheet.update_acell("M1", "Last Update (BJ):")
    sheet.update_acell("N1", now)
    print(f"🎉 任务完成！共推送 {len(df)} 只标的。")

if __name__ == "__main__":
    shares = get_a_share_list()
    if not shares.empty:
        results = scan_market_via_yfinance(shares)
        write_sheet(results)
