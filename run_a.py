import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, traceback
import yfinance as yf
import akshare as ak
import logging
import re

warnings.filterwarnings('ignore')
# 屏蔽 yfinance 内部烦人的警告输出
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

def get_worksheet():
    doc = client.open_by_url(OUTPUT_SHEET_URL)
    try:
        return doc.worksheet("A-Share Screener")
    except gspread.exceptions.WorksheetNotFound:
        return doc.add_worksheet(title="A-Share Screener", rows=100, cols=20)

# ==========================================
# ⚡ STEP 1: 获取 A 股名册 (闪电脱壳版 - 耗时<2秒)
# ==========================================
def get_a_share_list():
    print("\n🌍 [STEP 1] 启动【闪电脱壳机制】：直接获取 A 股纯净名册...")
    df = pd.DataFrame()
    try:
        # 🚀 极致优化：不拉取全市场实时价格，只拉取基础代码名册！无视防火墙阻截。
        df = ak.stock_info_a_code_name()
        df = df.rename(columns={"代码": "code", "名称": "name"})
        print("   -> ✅ 极速名册拉取成功！耗时不到 2 秒。")
    except Exception as e:
        print(f"   -> ⚠️ 极速接口受阻: {e}，启动备用【新浪大盘】接口 (约需 45 秒)...")
        try:
            df = ak.stock_zh_a_spot()
            df = df.rename(columns={"symbol": "code", "name": "name"})
        except Exception as e2:
            print(f"   -> ❌ 所有清单获取均失败: {e2}")
            return pd.DataFrame()

    # -----------------------------------------
    # 🔧 强制清洗代码，兼容带有 sh/sz 前缀的数据
    # -----------------------------------------
    df['code'] = df['code'].astype(str).str.extract(r'(\d{6})') # 只提取连续的 6 位数字
    df = df.dropna(subset=['code']) # 删掉提取不到数字的行
    
    # 剔除 ST 垃圾股
    df = df[~df['name'].astype(str).str.contains('ST')]
    
    # 过滤掉退市股或无效名称
    df = df[df['name'].astype(str).str.strip() != '']
    
    print(f"   -> ✅ 洗盘完毕！提取到全市场 {len(df)} 只纯净标的，进入高速演算通道。")
    return df[['code', 'name']]

# ==========================================
# 🚀 STEP 2: Yahoo Finance 全球数据中心并发盲扫
# ==========================================
def scan_market_via_yfinance(df_list):
    print("\n🚀[STEP 2] 启动【Yahoo Finance】天基武器，无视国内 WAF，执行高速大盘演算...")
    
    tickers = []
    ticker_to_name = {}
    
    # 转换 A 股代码为 Yahoo Finance 专属后缀 (.SS 为上海, .SZ 为深圳)
    for _, row in df_list.iterrows():
        c = str(row['code'])
        n = str(row['name'])
        
        # 严格分类：6开头的去上海，0或3开头的去深圳。忽略北交所(8/4)
        if c.startswith('6'): 
            t = f"{c}.SS"
        elif c.startswith(('0', '3')): 
            t = f"{c}.SZ"
        else: 
            continue
            
        tickers.append(t)
        ticker_to_name[t] = n
        
    print(f"   -> 📡 构建完成 {len(tickers)} 条数据通道，开始高维批量下载 (网速极快，请稍候)...")
    if not tickers:
        print("   -> ❌ 致命错误：转换后的 Yahoo Tickers 列表为空！")
        return []
    
    all_results = []
    chunk_size = 800 # 每次并发下载 800 只股票
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"   -> 📥 正在下载演算第 {i+1} ~ {min(i+chunk_size, len(tickers))} 只核心标的...")
        
        # 利用 Yahoo Finance 并发下载 1 年期前复权历史数据
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
                
                # yfinance 的 Volume 是股数，乘以价格即为成交额 (RMB)
                turnover_5 = np.mean(closes[-5:] * vols[-5:])
                if turnover_5 < 150000000: continue # 5日均额必须大于 1.5亿
                
                ma20 = np.mean(closes[-20:])
                ma50 = np.mean(closes[-50:])
                ma150 = np.mean(closes[-150:])
                ma200 = np.mean(closes[-200:])
                
                high60 = np.max(highs[-60:])
                h250 = np.max(highs[-250:]) if len(highs) >= 250 else np.max(highs)
                
                avg_v50 = np.mean(vols[-50:])
                if avg_v50 == 0: continue
                vol_ratio = vols[-1] / avg_v50
                
                deltas = np.diff(closes[-30:])
                gain = np.where(deltas > 0, deltas, 0)
                loss = np.where(deltas < 0, -deltas, 0)
                avg_gain = pd.Series(gain).ewm(com=13, adjust=False).mean().iloc[-1]
                avg_loss = pd.Series(loss).ewm(com=13, adjust=False).mean().iloc[-1]
                rsi = 100 if avg_loss == 0 else 100 - (100 / (1 + (avg_gain / avg_loss)))
                
                r20 = (closes[-1] - closes[-21]) / closes[-21]
                r60 = (closes[-1] - closes[-61]) / closes[-61]
                r120 = (closes[-1] - closes[-121]) / closes[-121]
                rs = r20*0.4 + r60*0.3 + r120*0.3
                
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
        # 按照 RS 评分从高到低排序，呈现真正的龙头
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
    print("\n========== A股猎手系统 V8.0 (闪电引擎版) ==========")
    df_list = get_a_share_list()
    if df_list.empty: return
    
    data = scan_market_via_yfinance(df_list)
    write_sheet(data)

if __name__ == "__main__":
    main()
