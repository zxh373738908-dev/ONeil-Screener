import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. Google Sheets 连接
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"  
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [美股] 动量龙头回调 专属扫描器 (全参数展示版)
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
    
    print("\n========== [2/3] 启动华尔街并发下载引擎 (极速版) ==========")
    data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)
    print("✅ 历史数据下载完成，进入量化雷达扫描...")

    tech_passed_stocks = []
    
    for ticker in tickers:
        try:
            if ticker not in data.columns.levels[0]:
                continue
                
            df = data[ticker].dropna()
            if len(df) < 200: continue
            
            close = float(df['Close'].iloc[-1])
            volume = float(df['Volume'].iloc[-1])
            
            # 基础流动性过滤
            if close < 15 or (close * volume) < 50000000: continue
            
            avg_vol_50 = df['Volume'].tail(50).mean()
            vol_ratio = volume / avg_vol_50 if avg_vol_50 > 0 else 0
            
            ma50 = df['Close'].tail(50).mean()
            ma200 = df['Close'].tail(200).mean()
            high_250 = df['High'].tail(250).max()
            
            # --- 核心指标计算 ---
            # 距离最高点回撤
            dist_high = (close - high_250) / high_250
            # 距离 50日线距离 (寻找支撑买点用)
            dist_50ma = (close - ma50) / ma50
            
            # 周期涨幅 (20天, 60天, 90天, 120天)
            ret_20d = (close - float(df['Close'].iloc[-21])) / float(df['Close'].iloc[-21]) if len(df) >= 21 else 0
            ret_60d = (close - float(df['Close'].iloc[-63])) / float(df['Close'].iloc[-63]) if len(df) >= 63 else 0
            ret_90d = (close - float(df['Close'].iloc[-90])) / float(df['Close'].iloc[-90]) if len(df) >= 90 else 0
            ret_120d = (close - float(df['Close'].iloc[-126])) / float(df['Close'].iloc[-126]) if len(df) >= 126 else 0
            
            # 动量加权评分 (复刻文章逻辑：0.2*20天 + 0.4*60天 + 0.4*120天)
            mom_score = (0.2 * ret_20d) + (0.4 * ret_60d) + (0.4 * ret_120d)
            
            # RSI 计算
            delta = df['Close'].tail(30).diff()
            up = delta.clip(lower=0)
            down = -1 * delta.clip(upper=0)
            ema_up = up.ewm(com=13, adjust=False).mean()
            ema_down = down.ewm(com=13, adjust=False).mean()
            rs = ema_up / ema_down
            rsi = float(100 - (100 / (1 + rs)).iloc[-1])

            # ====================================================
            # ⚡ 战术 3：动量龙头回调 (Momentum Dip Buy)
            # ====================================================
            is_momentum_dip = (
                (ret_120d >= 0.20 or ret_60d >= 0.15) and  # 中期动量强劲 (半年涨20%或2月涨15%)
                (ma50 > ma200) and                         # 长线多头排列
                (close >= ma50 * 0.95) and                 # 靠近50日线 (允许假跌破5%洗盘)
                (rsi <= 55) and                            # RSI回落至55以下，提供绝佳买点
                (dist_high >= -0.20)                       # 距高点回撤不超过20%，拒绝破位垃圾股
            )

            if not is_momentum_dip: continue

            # 暂存数据，全面记录以便 Google Sheets 显示
            tech_passed_stocks.append({
                "Ticker": ticker,
                "Mom_Score": round(mom_score * 100, 2),    # 用作核心排序权重
                "Price": round(close, 2),
                "RSI": round(rsi, 2),
                "Dist_50MA%": f"{round(dist_50ma * 100, 2)}%",  # 观察是否跌穿均线
                "Dist_High%": f"{round(dist_high * 100, 2)}%",  # 观察回撤深度
                "20D_Ret%": f"{round(ret_20d * 100, 2)}%",
                "60D_Ret%": f"{round(ret_60d * 100, 2)}%",
                "120D_Ret%": f"{round(ret_120d * 100, 2)}%",
                "Vol_Ratio": round(vol_ratio, 2),          # 观察洗盘是否缩量 (<1 为佳)
                "Turnover(M)": round((close * volume) / 1000000, 2)
            })
            
        except Exception as e:
            continue
            
    print(f"⚡ 技术面初筛完成，共选出 {len(tech_passed_stocks)} 只标的。")
    print("========== 正在进行基本面深度核查 (市值 > 20亿) ==========")
    
    final_stocks = []
    for stock in tech_passed_stocks:
        try:
            ticker_obj = yf.Ticker(stock["Ticker"])
            market_cap = ticker_obj.info.get('marketCap', 0)
            market_cap_b = market_cap / 1_000_000_000 
            
            if market_cap_b >= 2.0:
                stock["Market_Cap(B)"] = round(market_cap_b, 2)
                final_stocks.append(stock)
                print(f"🎯 战术锁定: {stock['Ticker']} | 综合动量评分: {stock['Mom_Score']} | RSI: {stock['RSI']}")
            else:
                pass
        except:
            continue
            
    return final_stocks

def write_to_sheet(sheet_name, final_stocks):
    print("\n========== [3/3] 开始同步至指挥中心 (Google Sheets) ==========")
    try:
        sheet = client.open_by_url(SHEET_URL).worksheet(sheet_name)
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            
            # 🔥 排序逻辑：直接按「文章提及的加权动量评分 (Mom_Score)」从高到低排序
            df = df.sort_values(by=['Mom_Score'], ascending=[False])
            
            # 调整列显示顺序 (符合看盘逻辑：先看评分，再看价格/买点，最后看历史涨幅)
            cols = [
                "Ticker", "Mom_Score", "Market_Cap(B)", "Price", 
                "RSI", "Dist_50MA%", "Dist_High%", 
                "20D_Ret%", "60D_Ret%", "120D_Ret%", 
                "Vol_Ratio", "Turnover(M)"
            ]
            df = df[cols]
            
            data_to_write = [df.columns.values.tolist()] + df.values.tolist()
            sheet.clear()
            sheet.update(values=data_to_write, range_name="A1")
            
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("N1", "Last Updated:")
            sheet.update_acell("O1", now_time)
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
    write_to_sheet("Screener", us_results)
