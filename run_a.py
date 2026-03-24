import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests
import yfinance as yf

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
# 🛡️ 筹码分布计算 (保持方案一算法)
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
# 🌍 STEP 1: 获取 A 股名册 (敌后渗透版)
# ==========================================
def get_a_share_list():
    print("\n🌍 [STEP 1] 启动【敌后渗透】：尝试多源获取 A 股名册...")
    
    # 路径 A: 尝试东财 HTTP 协议 (绕过部分 TLS 拦截)
    # 使用多个不同的推送服务器节点 (82, 95, 110)
    for node in ['82', '95', '110']:
        url = f"http://{node}.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": "1", "pz": "5000", "po": "1", "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2", "invt": "2", "fid": "f3",
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
            "fields": "f12,f14"
        }
        try:
            r = requests.get(url, params=params, timeout=15)
            data = r.json()
            stocks = data.get('data', {}).get('diff', [])
            if stocks:
                df = pd.DataFrame(stocks)
                df.columns = ['code', 'name']
                print(f"   -> ✅ 从东财节点 {node} 渗透成功！")
                return df_clean(df)
        except:
            print(f"   -> ⚠️ 节点 {node} 拒绝连接，尝试下一路径...")

    # 路径 B: 终极备份 - 从 GitHub 静态镜像拉取 (这在 GitHub Actions 里几乎 100% 成功)
    print("   -> 📡 尝试从 GitHub 静态镜像获取备用名单...")
    backup_urls = [
        "https://raw.githubusercontent.com/kxmo/china-stock-list/master/stock_list.csv",
        "https://raw.githubusercontent.com/waditu/tushare/master/tushare/stock/stock_info.csv"
    ]
    for b_url in backup_urls:
        try:
            df_b = pd.read_csv(b_url, encoding='utf-8')
            # 尝试识别代码和名称列
            code_col = [c for c in df_b.columns if 'code' in c.lower() or '代码' in c][0]
            name_col = [c for c in df_b.columns if 'name' in c.lower() or '名称' in c][0]
            df = df_b[[code_col, name_col]].rename(columns={code_col:'code', name_col:'name'})
            print(f"   -> ✅ 从静态镜像 {b_url[:30]}... 渗透成功！")
            return df_clean(df)
        except:
            continue

    print("❌ 致命错误：所有渗透路径均被阻断。")
    return pd.DataFrame()

def df_clean(df):
    df['code'] = df['code'].astype(str).str.extract(r'(\d{6})')
    df = df.dropna(subset=['code'])
    df = df[df['code'].str.match(r'^(60|68|00|30)')]
    df = df[~df['name'].astype(str).str.contains('ST|退', case=False)]
    return df

# ==========================================
# 🚀 STEP 2: 原装方案 + 筹码确认 (保持逻辑原样)
# ==========================================
def scan_market_via_yfinance(df_list):
    print("\n🚀[STEP 2] 启动【T.U.A.W】原装战法 + 筹码分布确认...")
    
    tickers = []
    ticker_to_name = {}
    for _, row in df_list.iterrows():
        c = str(row['code'])
        t = f"{c}.SS" if c.startswith('6') else f"{c}.SZ"
        tickers.append(t)
        ticker_to_name[t] = row['name']
        
    all_results = []
    chunk_size = 400 # 减小块大小，增加稳定性
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"   -> 📥 分析进度: {i}/{len(tickers)}...")
        try:
            # yfinance 走的是 Yahoo 美国接口，GitHub 访问极其稳定
            data = yf.download(chunk, period="1y", auto_adjust=True, threads=True, progress=False)
        except: continue
        
        for ticker in chunk:
            try:
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
                
                # 您的原装门槛逻辑
                turnover_1 = price * vols[-1]
                turnover_5 = np.mean(closes[-5:] * vols[-5:])
                if turnover_5 < 100_000_000 or price < 5: continue 
                
                # 均线与指标
                ma20, ma50 = np.mean(closes[-20:]), np.mean(closes[-50:])
                ma150, ma200 = np.mean(closes[-150:]), np.mean(closes[-200:])
                h250 = np.max(highs[-250:])
                vol_ratio = vols[-1] / np.mean(vols[-50:])
                
                # 原装 RS 评分逻辑
                r20, r60, r120 = (price/closes[-21]-1), (price/closes[-61]-1), (price/closes[-121]-1)
                rs_score = (r20*0.4 + r60*0.3 + r120*0.3) * 100
                dist_high_pct = ((price - h250) / h250) * 100
                avg_amp5 = np.mean((highs[-5:] - lows[-5:]) / lows[-5:] * 100)

                # =========================================
                # ⚔️ 判定逻辑 (原封不动还原您的战法)
                # =========================================
                cond_mom = (rs_score > 85) or (r60 * 100 > 30)
                cond_to = 300_000_000 <= turnover_1 <= 1_500_000_000
                
                # 判定
                fuse = (-8 <= dist_high_pct <= -1) and (vol_ratio < 1.0) and (avg_amp5 < 5.0) and cond_mom and cond_to
                sniper = (-8 <= dist_high_pct <= 2) and (vol_ratio > 1.5) and cond_mom and cond_to and (price > ma20)
                breakout = (price > ma20 and price > ma50 and ma50 > ma150 and ma150 > ma200 and vol_ratio > 1.5)
                
                if not (fuse or sniper or breakout): continue
                
                type_label = "🔥 狙击触发" if sniper else ("🧨 引信雷达" if fuse else "🚀 趋势突破")
                
                # 筹码确认
                poc, res = get_chip_data(df_t)
                
                all_results.append({
                    "Ticker": ticker.split('.')[0], "Name": ticker_to_name[ticker],
                    "Price": round(price, 2), "Type": type_label,
                    "RS_Score": round(rs_score, 2), "POC(筹码中心)": poc,
                    "上方抛压%": res, "Vol_Ratio": round(vol_ratio, 2),
                    "Dist_High%": f"{round(dist_high_pct, 2)}%",
                    "Turnover(亿)": round(turnover_1 / 100000000, 2)
                })
            except: continue
                
    return all_results

# ==========================================
# 📝 STEP 3: 写入
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
    print(f"🎉 任务完成！")

if __name__ == "__main__":
    shares = get_a_share_list()
    if not shares.empty:
        results = scan_market_via_yfinance(shares)
        write_sheet(results)
