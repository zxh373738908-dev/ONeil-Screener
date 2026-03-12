import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, traceback
import yfinance as yf
import akshare as ak
import logging

warnings.filterwarnings('ignore')
# 屏蔽 yfinance 内部烦人的警告输出
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"

scopes =["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

def get_worksheet():
    doc = client.open_by_url(OUTPUT_SHEET_URL)
    try:
        return doc.worksheet("A-Share Screener")
    except gspread.exceptions.WorksheetNotFound:
        return doc.add_worksheet(title="A-Share Screener", rows=100, cols=20)

# ==========================================
# 🛡️ STEP 1: 获取 A 股名册 (三重容错脱壳机制)
# ==========================================
def get_a_share_list():
    print("\n🌍 [STEP 1] 启动【底层脱壳机制】：从云端获取全市场名册...")
    
    # 【首选】方案 1: 东方财富接口 (速度极快，防封锁能力极强)
    try:
        print("   -> 尝试获取东方财富全市场 A 股...")
        df = ak.stock_zh_a_spot_em()
        if not df.empty and "代码" in df.columns and "名称" in df.columns:
            df = df.rename(columns={"代码": "code", "名称": "name"})
            df['code'] = df['code'].astype(str).str.zfill(6) # 安全补齐6位数
            df = df[~df['name'].astype(str).str.contains('ST')]
            print(f"   -> ✅ 成功通过东方财富接口拉取 {len(df)} 只标的！")
            return df[['code', 'name']]
    except Exception as e:
        print(f"   -> ⚠️ 东方财富接口受阻: {e}")

    # 【备选】方案 2: 官方 A 股名册
    try:
        print("   -> 尝试获取官方 A 股名册...")
        df = ak.stock_info_a_code_name()
        if not df.empty:
            if "代码" in df.columns and "名称" in df.columns:
                df = df.rename(columns={"代码": "code", "名称": "name"})
            df['code'] = df['code'].astype(str).str.zfill(6)
            df = df[~df['name'].astype(str).str.contains('ST')]
            print(f"   -> ✅ 成功拉取到全市场 {len(df)} 只正常交易标的。")
            return df[['code', 'name']]
    except Exception as e:
        print(f"   -> ⚠️ 官方名册接口受阻: {e}")

    # 【底牌】方案 3: 交易所直连
    try:
        print("   -> 尝试组合上交所/深交所列表...")
        sh_df = ak.stock_info_sh_name_code(indicator="主板A股")
        sz_df = ak.stock_info_sz_name_code(indicator="A股列表")
        
        sh = sh_df.iloc[:, [0, 1]].copy()
        sh.columns = ['code', 'name']
        sz = sz_df.iloc[:, [0, 1]].copy()
        sz.columns = ['code', 'name']
        
        df = pd.concat([sh, sz], ignore_index=True)
        df['code'] = df['code'].astype(str).str.zfill(6)
        df = df[~df['name'].astype(str).str.contains('ST')]
        print(f"   -> ✅ 成功绕过封锁！拉取到全市场 {len(df)} 只正常交易标的。")
        return df[['code', 'name']]
    except Exception as e:
        print(f"   -> ⚠️ 交易所直连受阻: {e}")

    print("   -> ❌ 所有获取接口均失败！请检查网络状态或稍后再试。")
    return pd.DataFrame()

# ==========================================
# 🚀 STEP 2: Yahoo Finance 全球数据中心并发盲扫
# ==========================================
def scan_market_via_yfinance(df_list):
    print("\n🚀[STEP 2] 启动【Yahoo Finance】天基武器，无视国内 WAF，执行高速大盘演算...")
    
    tickers = []
    ticker_to_name = {}
    
    for _, row in df_list.iterrows():
        c = row['code']
        n = row['name']
        if c.startswith(('6', '5')): t = f"{c}.SS"
        elif c.startswith(('0', '3')): t = f"{c}.SZ"
        else: continue
        tickers.append(t)
        ticker_to_name[t] = n
        
    print(f"   -> 📡 构建完成 {len(tickers)} 条数据通道，开始高维批量下载 (网速极快，请稍候)...")
    
    all_results = []
    chunk_size = 1000 
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"   -> 📥 正在下载演算第 {i+1} ~ {min(i+chunk_size, len(tickers))} 只核心标的...")
        
        # 使用 Yahoo Finance 下载 1 年期历史数据
        data = yf.download(chunk, period="1y", auto_adjust=True, threads=True, progress=False)
        
        for ticker in chunk:
            try:
                # 兼容 yfinance DataFrame 的多层索引结构
                if len(chunk) > 1:
                    closes = data['Close'][ticker].dropna().values
                    highs = data['High'][ticker].dropna().values
                    vols = data['Volume'][ticker].dropna().values
                else:
                    closes = data['Close'].dropna().values
                    highs = data['High'].dropna().values
                    vols = data['Volume'].dropna().values
                    
                if len(closes) < 200: continue
                
                price = closes[-1]
                if price < 5: continue
                
                # 5日平均成交额 > 1.5 亿
                turnover_5 = np.mean(closes[-5:] * vols[-5:])
                if turnover_5 < 150000000: continue 
                
                ma20 = np.mean(closes[-20:])
                ma50 = np.mean(closes[-50:])
                ma150 = np.mean(closes[-150:])
                ma200 = np.mean(closes[-200:])
                
                high60 = np.max(highs[-60:])
                h250 = np.max(highs[-250:]) if len(highs) >= 250 else np.max(highs)
                
                avg_v50 = np.mean(vols[-50:])
                if avg_v50 == 0: continue
                vol_ratio = vols[-1] / avg_v50
                
                # RSI
                deltas = np.diff(closes[-30:])
                gain = np.where(deltas > 0, deltas, 0)
                loss = np.where(deltas < 0, -deltas, 0)
                avg_gain = pd.Series(gain).ewm(com=13, adjust=False).mean().iloc[-1]
                avg_loss = pd.Series(loss).ewm(com=13, adjust=False).mean().iloc[-1]
                rsi = 100 if avg_loss == 0 else 100 - (100 / (1 + (avg_gain / avg_loss)))
                
                # RS 模型
                r20 = (closes[-1] - closes[-21]) / closes[-21]
                r60 = (closes[-1] - closes[-61]) / closes[-61]
                r120 = (closes[-1] - closes[-121]) / closes[-121]
                rs = r20*0.4 + r60*0.3 + r120*0.3
                
                # 战法判定
                breakout = (price > ma20 and price > ma50 and ma50 > ma150 and ma150 > ma200 and vol_ratio > 1.5 and rsi > 60)
                ambush = (abs(price - ma20) / ma20 < 0.03 and vol_ratio < 1.1 and ma50 > ma150 and ma150 > ma200)
                pit = (price < high60 * 0.85 and price >= ma50 * 0.98 and vol_ratio > 1.3)
                
                if not (breakout or ambush or pit): continue
                
                type_label = "🐉 黄金坑" if pit else ("🔥 突破起飞" if breakout else "🧘 缩量伏击")
                
                all_results.append({
                    "Ticker": ticker.split('.')[0], 
                    "Name": ticker_to_name[ticker],
                    "Price": round(price, 2),
                    "Type": type_label,
                    "RS_Score": round(rs * 100, 2),
                    "RSI": round(rsi, 2),
                    "Vol_Ratio": round(vol_ratio, 2),
                    "Dist_High%": f"{round(((price - h250) / h250) * 100, 2)}%",
                    "Turnover(亿)": round(turnover_5 / 100000000, 2)
                })
            except Exception:
                continue
                
    return all_results

# ==========================================
# 📝 STEP 3: 写入作战指令
# ==========================================
def write_sheet(data):
    print("\n📝 [STEP 3] 正在将绝密作战名单写入 Google Sheets 表格...")
    try:
        sheet = get_worksheet()
        sheet.clear()

        if len(data) == 0:
            sheet.update_acell("A1", "No Signal: 战局恶劣或处于洗盘期，暂无极品标的。")
            print("⚠️ 筛选完毕，已写入空仓报告！")
            return

        df = pd.DataFrame(data)
        df = df.sort_values("RS_Score", ascending=False).head(50)

        sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sheet.update_acell("K1", "Last Update:")
        sheet.update_acell("L1", now)
        
        print(f"🎉 大功告成！已成功将 {len(df)} 只战法认证龙头送达指挥部！")
    except Exception as e:
        print(f"❌ 表格写入失败: {e}")

# ==========================================
# MAIN
# ==========================================
def main():
    print("\n========== A股猎手系统 V7.2 (天基武器防壁破甲版) ==========")
    df_list = get_a_share_list()
    if df_list.empty: 
        print("程序终止：未能获取股票列表。")
        return
    
    data = scan_market_via_yfinance(df_list)
    write_sheet(data)

if __name__ == "__main__":
    main()
