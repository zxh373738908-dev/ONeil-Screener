import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests
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
# 🌍 STEP 1: 获取 A 股名册 (GitHub 镜像穿透版)
# ==========================================
def get_a_share_list():
    print("\n🌍 [STEP 1] 启动【GitHub 内部镜像穿透】获取 A 股名册...")
    
    # 使用多个可靠的 GitHub 每日自动同步的股票 CSV 镜像
    mirror_urls = [
        # 镜像 1 (由国内开发者自动推送到 GitHub 的清单)
        "https://raw.githubusercontent.com/shilei-v5/StockList/main/china_stock_list.csv",
        # 镜像 2 (备份镜像)
        "https://raw.githubusercontent.com/crawstock/stock/master/stock_list.csv"
    ]
    
    for url in mirror_urls:
        try:
            print(f"   -> 📡 尝试从 GitHub 静态仓库拉取: {url[:50]}...")
            df = pd.read_csv(url, dtype={'code': str, '代码': str})
            
            # 列名清洗适配
            if '代码' in df.columns: df = df.rename(columns={'代码': 'code', '名称': 'name'})
            if 'symbol' in df.columns: df = df.rename(columns={'symbol': 'code'})
            
            df = df[['code', 'name']]
            df['code'] = df['code'].str.extract(r'(\d{6})')
            df = df.dropna(subset=['code'])
            
            # 过滤 A 股常用号段
            df = df[df['code'].str.match(r'^(60|68|00|30)')]
            df = df[~df['name'].astype(str).str.contains('ST|退', case=False)]
            
            if len(df) > 1000:
                print(f"   -> ✅ 名册拉取成功！共 {len(df)} 只标的。")
                return df
        except Exception as e:
            print(f"   -> ⚠️ 此路径受阻: {e}")
            continue

    # 如果所有镜像都挂了，使用最笨但最稳的【种子生成器】逻辑 (确保程序不崩溃)
    print("   -> 🚨 警告：所有远程清单失效，启动【核心成分股种子】模式...")
    # 这里手动列出一部分具有代表性的代码，或者通过生成器生成号段
    seeds = [{'code': str(i).zfill(6), 'name': 'Seed_Stock'} for i in range(600000, 600100)]
    return pd.DataFrame(seeds)

# ==========================================
# 🚀 STEP 2: 原装方案 + 筹码确认 (维持 T.U.A.W. 逻辑)
# ==========================================
def scan_market_via_yfinance(df_list):
    print("\n🚀[STEP 2] 启动【T.U.A.W】原装战法 + 筹码分布确认...")
    
    tickers = []
    ticker_to_name = {}
    for _, row in df_list.iterrows():
        c = str(row['code'])
        # A股分类规则
        t = f"{c}.SS" if c.startswith(('6')) else f"{c}.SZ"
        tickers.append(t)
        ticker_to_name[t] = row['name']
        
    all_results = []
    chunk_size = 400 
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"   -> 📥 扫描进度: {i}/{len(tickers)} (Yahoo Finance 数据通道)...")
        try:
            # yfinance 走美国 Yahoo 接口，GitHub 访问极其稳定
            data = yf.download(chunk, period="1y", auto_adjust=True, threads=True, progress=False)
        except: continue
        
        for ticker in chunk:
            try:
                # 解包数据
                if len(chunk) > 1:
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
                
                # 成交额容错计算 (基础门槛)
                turnover_1 = price * vols[-1]
                turnover_5 = np.mean(closes[-5:] * vols[-5:])
                if turnover_5 < 100000000 or price < 5: continue 
                
                # 均线与高点计算
                ma20, ma50 = np.mean(closes[-20:]), np.mean(closes[-50:])
                ma150, ma200 = np.mean(closes[-150:]), np.mean(closes[-200:])
                h250 = np.max(highs[-250:])
                vol_ratio = vols[-1] / np.mean(vols[-50:])
                
                # RSI计算
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
                # ⚔️ 核心逻辑判定 (完全还原原版)
                # =========================================
                cond_mom = (rs_score > 85) or (r60 * 100 > 30)
                cond_to = 300_000_000 <= turnover_1 <= 1_500_000_000
                
                # 判定
                fuse = (-8 <= dist_high_pct <= -1) and (vol_ratio < 1.0) and (avg_amp5 < 5.0) and cond_mom and cond_to
                sniper = (-8 <= dist_high_pct <= 2) and (vol_ratio > 1.5) and cond_mom and cond_to and (price > ma20)
                breakout = (price > ma20 and price > ma50 and ma50 > ma150 and ma150 > ma200 and vol_ratio > 1.5 and rsi > 60)
                ambush = (abs(price - ma20) / ma20 < 0.03 and vol_ratio < 1.1 and ma50 > ma150 and ma150 > ma200)

                if not (fuse or sniper or breakout or ambush): continue
                
                # 授予战术评级
                if sniper: type_label = "🔥 狙击触发"
                elif fuse: type_label = "🧨 引信雷达"
                elif breakout: type_label = "🚀 趋势突破"
                else: type_label = "🧘 均线伏击"
                
                # 筹码确认 (方案一算法)
                poc, res = get_chip_data(df_t)
                
                all_results.append({
                    "Ticker": ticker.split('.')[0],
                    "Name": ticker_to_name[ticker],
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
                
    return all_results

# ==========================================
# 📝 STEP 3: 写入作战名单
# ==========================================
def write_sheet(data):
    sheet = get_worksheet()
    if not sheet: return
    sheet.clear()
    if not data:
        sheet.update_acell("A1", "今日无动量共振标的。")
        return
    df = pd.DataFrame(data).sort_values("RS_Score", ascending=False).head(50)
    sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
    
    tz_bj = datetime.timezone(datetime.timedelta(hours=8))
    now = datetime.datetime.now(tz=tz_bj).strftime("%Y-%m-%d %H:%M:%S")
    sheet.update_acell("M1", f"Last Update (BJ): {now}")
    print(f"🎉 任务完美完成！数据已送达 Google Sheets。")

if __name__ == "__main__":
    shares = get_a_share_list()
    if not shares.empty:
        results = scan_market_via_yfinance(shares)
        write_sheet(results)
