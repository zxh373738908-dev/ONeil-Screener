import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. Google Sheets 连接 (保持不变)
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"  
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [美股] 欧奈尔突破 + 老龙回头 双核扫描器
# ==========================================
def get_us_tickers():
    print("\n========== [1/3] 开始获取美股核心目标群 ==========")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        sp_tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)
        sp500 = next(df['Symbol'].tolist() for df in sp_tables if 'Symbol' in df.columns)
        
        ndx_tables = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100', storage_options=headers)
        ndx100 = next(df['Ticker'].tolist() for df in ndx_tables if 'Ticker' in df.columns)
        
        tickers = list(set([str(t).replace('.', '-') for t in (sp500 + ndx100)]))
        print(f"✅ 成功获取并去重，共计 {len(tickers)} 只核心股票。")
        return tickers
    except Exception as e:
        print(f"❌ 获取美股列表失败: {e}")
        return []

def screen_us_stocks():
    tickers = get_us_tickers()
    if not tickers: return []
    
    print("\n========== [2/3] 启动华尔街并发下载引擎 (防封锁) ==========")
    # 批量并发下载，速度提升 20 倍，且大大降低被雅虎封锁的概率
    data = yf.download(tickers, period="1y", group_by='ticker', threads=True, show_errors=False)
    print("✅ 历史数据下载完成，进入量化雷达扫描...")

    final_stocks = []
    
    for ticker in tickers:
        try:
            # 提取单只股票数据并清理 NaN
            df = data[ticker].dropna()
            if len(df) < 200: continue
            
            close = df['Close'].iloc[-1]
            volume = df['Volume'].iloc[-1]
            high = df['High'].iloc[-1]
            
            # 流动性过滤：股价 < 15 或 成交额 < 5000万美金，直接跳过
            if close < 15 or (close * volume) < 50000000: continue
            
            # --- 基础指标计算 ---
            avg_vol_50 = df['Volume'].tail(50).mean()
            vol_ratio = volume / avg_vol_50 if avg_vol_50 > 0 else 0
            
            ma20 = df['Close'].rolling(20).mean().iloc[-1]
            ma50 = df['Close'].rolling(50).mean().iloc[-1]
            ma150 = df['Close'].rolling(150).mean().iloc[-1]
            ma200 = df['Close'].rolling(200).mean().iloc[-1]
            high_250 = df['High'].rolling(250).max().iloc[-1]
            
            # 距离 52周新高的回撤幅度 (负数)
            dist_high = (close - high_250) / high_250
            
            # 收益率计算 (90天约 63 个交易日， 半年约 126 个交易日)
            ret_90d = (close - df['Close'].iloc[-63]) / df['Close'].iloc[-63]
            ret_120d = (close - df['Close'].iloc[-126]) / df['Close'].iloc[-126] if len(df) >= 126 else ret_90d
            
            # RSI 计算
            delta = df['Close'].diff()
            up = delta.clip(lower=0)
            down = -1 * delta.clip(upper=0)
            ema_up = up.ewm(com=13, adjust=False).mean().iloc[-1]
            ema_down = down.ewm(com=13, adjust=False).mean().iloc[-1]
            rs = ema_up / ema_down if ema_down > 0 else 100
            rsi = 100 - (100 / (1 + rs))

            # ====================================================
            # ⚔️ 战术 1：欧奈尔主升突破 (Breakout) - 蓝天无阻力区
            # ====================================================
            is_breakout = (
                (ma50 > ma200) and 
                (close > ma150 and close > ma200) and
                (close >= high_250 * 0.80) and  # 距离最高点不超 20%
                (ret_90d >= 0.15) and          # 3个月涨幅 > 15%
                (rsi >= 45) and
                (close > ma20)                 # 站在 20 日线之上 (强势)
            )

            # ====================================================
            # 🧬 战术 2：老龙回头 (Pullback to 50-Day Line) - 机构护盘点
            # ====================================================
            is_pullback = (
                (ret_120d >= 0.20) and         # 底蕴：半年暴涨超 20% 的真老龙
                (-0.20 <= dist_high <= -0.10) and # 洗盘：距高点回撤 10%-20%，完美黄金坑
                (close < ma20) and             # 破位：跌破 20 日均线，逼出散户恐慌盘
                (ma50 <= close <= ma50 * 1.03) and # 支撑：死死踩在 50 日均线之上（误差 3% 内）
                (vol_ratio >= 1.5)             # 点火：右侧爆出 1.5 倍以上巨量，机构护盘！
            )

            # 如果均不满足，直接过滤
            if not (is_breakout or is_pullback): continue

            # 结构标签识别
            if is_pullback and is_breakout:
                struct_label = "🔥 终极双核 (Breakout + 50D Rebound)"
            elif is_pullback:
                struct_label = "🐉 老龙回头 (Pullback to SMA50)"
            else:
                struct_label = "🚀 强势突破 (>SMA20)"

            final_stocks.append({
                "Ticker": ticker,
                "Price": round(close, 2),
                "90D_Return%": f"{round(ret_90d * 100, 2)}%",
                "RSI": round(rsi, 2),
                "Vol_Ratio": round(vol_ratio, 2),
                "Dist_High%": f"{round(dist_high * 100, 2)}%",
                "Turnover(M)": round((close * volume) / 1000000, 2),
                "Struct": struct_label
            })
            print(f"🎯 捕获战术标的: {ticker} [{struct_label}]")
            
        except Exception as e:
            # 批量下载出错率极低，即使报错单只股票也可安全忽略
            continue
            
    return final_stocks

def write_to_sheet(sheet_name, final_stocks):
    print("\n========== [3/3] 开始同步至指挥中心 (Google Sheets) ==========")
    try:
        sheet = client.open_by_url(SHEET_URL).worksheet(sheet_name)
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            # 排序逻辑：优先按量比(Vol_Ratio)和90天涨幅排序，确保爆量老龙排在前面
            df['Sort_Num'] = df['90D_Return%'].str.replace('%', '').astype(float)
            df = df.sort_values(by=['Vol_Ratio', 'Sort_Num'], ascending=[False, False]).drop(columns=['Sort_Num'])
            
            data_to_write = [df.columns.values.tolist()] + df.values.tolist()
            sheet.clear()
            sheet.update(values=data_to_write, range_name="A1")
            
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("I1", "Last Updated:")
            sheet.update_acell("J1", now_time)
            print(f"🎉 成功！{len(df)} 只最强龙头已装填至 {sheet_name}！")
        else:
            sheet.clear()
            msg = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 当下无符合条件的股票，大盘极度恶劣，防守为主。"
            sheet.update_acell("A1", msg)
            print(f"⚠️ {sheet_name}: 已输出空仓警告。")
    except Exception as e:
        print(f"❌ 写入 {sheet_name} 失败: {e}")

if __name__ == "__main__":
    us_results = screen_us_stocks()
    # 写入表格，按照新的组合逻辑输出
    write_to_sheet("Screener", us_results)
