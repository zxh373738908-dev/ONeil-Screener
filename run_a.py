import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, traceback, logging
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
# 🛡️ 核心补丁：筹码分布计算 (模拟 TradingView)
# ==========================================
def get_chip_data(df_ticker, lookback=120):
    """模拟 TradingView 筹码分布数据"""
    try:
        hist = df_ticker.tail(lookback)
        p_min, p_max = hist['Low'].min(), hist['High'].max()
        bins = 40  # 40层价格峰
        price_range = np.linspace(p_min, p_max, bins + 1)
        v_dist = np.zeros(bins)
        
        for _, row in hist.iterrows():
            idx = np.where((price_range[:-1] >= row['Low']) & (price_range[1:] <= row['High']))[0]
            if len(idx) > 0:
                v_dist[idx] += row['Volume'] / len(idx)
            else:
                c_idx = np.searchsorted(price_range, row['Close']) - 1
                if 0 <= c_idx < bins: v_dist[c_idx] += row['Volume']
        
        # 寻找 POC
        poc_price = (price_range[np.argmax(v_dist)] + price_range[np.argmax(v_dist)+1]) / 2
        
        # 计算上方阻力占比 (Overhead Resistance)
        curr_price = df_ticker['Close'].iloc[-1]
        curr_idx = np.searchsorted(price_range, curr_price) - 1
        overhead_vol = np.sum(v_dist[curr_idx:]) if curr_idx < bins else 0
        total_vol = np.sum(v_dist)
        res_ratio = (overhead_vol / total_vol) * 100 if total_vol > 0 else 0
        
        return round(poc_price, 2), f"{round(res_ratio, 1)}%"
    except:
        return 0, "N/A"

# ==========================================
# 🌍 STEP 1: 获取 A 股名册 (抗封锁增强版)
# ==========================================
def get_a_share_list():
    print("\n🌍 [STEP 1] 启动底层脱壳机制：获取 A 股纯净名册...")
    df = pd.DataFrame()
    
    for attempt in range(3):
        try:
            # 使用东财接口，由于其相对 Sina 更稳定，适合 GitHub Actions 运行
            df = ak.stock_zh_a_spot_em()
            if not df.empty:
                print(f"   -> ✅ 接口拉取成功！(尝试第 {attempt+1} 次)")
                break
        except Exception as e:
            print(f"   -> ⚠️ 尝试 {attempt+1} 失败，正在重试...")
            time.sleep(5)

    if df.empty:
        print("❌ 致命错误：无法获取股票清单。")
        return pd.DataFrame()

    # 适配列名
    col_map = {'代码': 'code', '名称': 'name', '最新价': 'price'}
    df = df.rename(columns=col_map)
    
    # 清洗：只保留 6, 0, 3 开头的代码，剔除 ST
    df['code'] = df['code'].astype(str)
    df = df[df['code'].str.match(r'^(60|68|00|30)')]
    df = df[~df['name'].astype(str).str.contains('ST|退', case=False)]
    
    print(f"   -> ✅ 洗盘完毕！锁定 {len(df)} 只标的进入演算通道。")
    return df[['code', 'name']]

# ==========================================
# 🚀 STEP 2: 原装战法扫描仪 + 筹码确认
# ==========================================
def scan_market_via_yfinance(df_list):
    print("\n🚀[STEP 2] 启动【T.U.A.W】原装战法 + 筹码确认...")
    
    tickers = []
    ticker_to_name = {}
    for _, row in df_list.iterrows():
        c = str(row['code'])
        t = f"{c}.SS" if c.startswith('6') else f"{c}.SZ"
        tickers.append(t)
        ticker_to_name[t] = row['name']
        
    all_results = []
    chunk_size = 500  # 为保证稳定性，缩小分块大小
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"   -> 📥 正在演算区块 {i//chunk_size + 1}...")
        
        try:
            data = yf.download(chunk, period="1y", auto_adjust=True, threads=True, progress=False)
        except:
            continue
        
        for ticker in chunk:
            try:
                # 提取个股数据并清洗
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
                
                # 成交额容错判定
                turnover_1 = price * vols[-1]
                turnover_5 = np.mean(closes[-5:] * vols[-5:])
                if turnover_5 < 100000000 or price < 5: continue 
                
                # 1. 均线与高点
                ma20, ma50 = np.mean(closes[-20:]), np.mean(closes[-50:])
                ma150, ma200 = np.mean(closes[-150:]), np.mean(closes[-200:])
                h250 = np.max(highs[-250:])
                
                # 2. 量比
                vol_ratio = vols[-1] / np.mean(vols[-50:])
                
                # 3. RSI
                deltas = np.diff(closes[-30:])
                gain = np.where(deltas > 0, deltas, 0)
                loss = np.where(deltas < 0, -deltas, 0)
                avg_gain = pd.Series(gain).ewm(com=13, adjust=False).mean().iloc[-1]
                avg_loss = pd.Series(loss).ewm(com=13, adjust=False).mean().iloc[-1]
                rsi = 100 if avg_loss == 0 else 100 - (100 / (1 + (avg_gain / avg_loss)))
                
                # 4. 动量 RS (您的核心逻辑)
                r20, r60, r120 = (price/closes[-21]-1), (price/closes[-61]-1), (price/closes[-121]-1)
                rs_score = (r20*0.4 + r60*0.3 + r120*0.3) * 100
                dist_high_pct = ((price - h250) / h250) * 100

                # 5. VCP 振幅判定
                amps = (highs[-5:] - lows[-5:]) / lows[-5:] * 100
                avg_amp5 = np.mean(amps)

                # =========================================
                # ⚔️ 核心战法逻辑判决 (完全保留原版)
                # =========================================
                cond_dist_radar = -8 <= dist_high_pct <= -1
                cond_vcp = (vol_ratio < 1.0) and (avg_amp5 < 5.0)
                cond_momentum = (rs_score > 85) or (r60 * 100 > 30)
                cond_turnover = 300_000_000 <= turnover_1 <= 1_500_000_000

                # [战法1] 引信雷达
                fuse_radar = cond_dist_radar and cond_vcp and cond_momentum and cond_turnover
                # [战法2] 狙击触发
                trigger_sniper = (-8 <= dist_high_pct <= 2) and (vol_ratio > 1.5) and cond_momentum and cond_turnover and (price > ma20)
                # [战法3] 经典突破
                breakout = (price > ma20 and price > ma50 and ma50 > ma150 and ma150 > ma200 and vol_ratio > 1.5 and rsi > 60)
                # [战法4] 均线伏击
                ambush = (abs(price - ma20) / ma20 < 0.03 and vol_ratio < 1.1 and ma50 > ma150 and ma150 > ma200)

                if not (fuse_radar or trigger_sniper or breakout or ambush): 
                    continue
                
                # 授予评级标签
                if trigger_sniper: type_label = "🔥 狙击触发"
                elif fuse_radar: type_label = "🧨 引信雷达"
                elif breakout: type_label = "🚀 趋势突破"
                else: type_label = "🧘 均线伏击"
                
                # -----------------------------------------
                # 👁️ 【方案一】集成：筹码峰二次确认
                # -----------------------------------------
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
            except:
                continue
                
    return all_results

# ==========================================
# 📝 STEP 3: 写入 Google Sheets
# ==========================================
def write_sheet(data):
    print("\n📝 [STEP 3] 正在同步作战名单至 Google Sheets...")
    sheet = get_worksheet()
    if not sheet: return
    
    sheet.clear()
    if not data:
        sheet.update_acell("A1", "今日无动量+筹码共振标的。")
        return

    df = pd.DataFrame(data)
    # 按 RS 评分排序，取前 50 只
    df = df.sort_values("RS_Score", ascending=False).head(50)
    
    sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
    
    # 强制北京时间显示
    tz_bj = datetime.timezone(datetime.timedelta(hours=8))
    now = datetime.datetime.now(tz=tz_bj).strftime("%Y-%m-%d %H:%M:%S")
    sheet.update_acell("M1", "Last Update (BJ Time):")
    sheet.update_acell("N1", now)
    print(f"🎉 任务完美完成！共推送 {len(df)} 只绝密标的。")

# ==========================================
# 入口
# ==========================================
if __name__ == "__main__":
    print(f"\n{'='*40}\n   A股猎手 V8.4 - 动量/筹码上帝视角\n{'='*40}")
    shares = get_a_share_list()
    if not shares.empty:
        results = scan_market_via_yfinance(shares)
        write_sheet(results)
