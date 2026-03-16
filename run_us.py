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
# 2. [美股] 量化多核扫描器
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
            
            if close < 15 or (close * volume) < 50000000: continue
            
            avg_vol_50 = df['Volume'].tail(50).mean()
            vol_ratio = volume / avg_vol_50 if avg_vol_50 > 0 else 0
            
            ma20 = df['Close'].tail(20).mean()
            ma50 = df['Close'].tail(50).mean()
            ma150 = df['Close'].tail(150).mean()
            ma200 = df['Close'].tail(200).mean()
            high_250 = df['High'].tail(250).max()
            
            dist_high = (close - high_250) / high_250
            
            ret_90d = (close - float(df['Close'].iloc[-63])) / float(df['Close'].iloc[-63])
            ret_120d = (close - float(df['Close'].iloc[-126])) / float(df['Close'].iloc[-126]) if len(df) >= 126 else ret_90d
            
            delta = df['Close'].tail(30).diff()
            up = delta.clip(lower=0)
            down = -1 * delta.clip(upper=0)
            ema_up = up.ewm(com=13, adjust=False).mean()
            ema_down = down.ewm(com=13, adjust=False).mean()
            rs = ema_up / ema_down
            rsi = float(100 - (100 / (1 + rs)).iloc[-1])

            # ====================================================
            # ⚔️ 战术 1：欧奈尔主升突破 (Breakout) - 蓝天无阻力区
            # ====================================================
            is_breakout = (
                (ma50 > ma200) and 
                (close > ma150 and close > ma200) and
                (dist_high >= -0.15) and       
                (ret_90d >= 0.15) and          
                (rsi >= 55) and                
                (close > ma20)                 
            )

            # ====================================================
            # 🧬 战术 2：老龙回头专属核心逻辑 (Pullback to 50-Day Line)
            # ====================================================
            is_pullback = (
                (ret_90d >= 0.15 or ret_120d >= 0.15) and 
                (-0.20 <= dist_high <= -0.10) and         
                (ma50 * 0.97 <= close <= ma50 * 1.05) and 
                (close < ma20)                            
            )

            # ====================================================
            # ⚡ 战术 3 (新增)：动量龙头回调 (Momentum Dip Buy)
            # 融合上一篇文章思想：寻找中期动量极强，但短期RSI回落的标的
            # ====================================================
            is_momentum_dip = (
                (ret_120d >= 0.25 or ret_90d >= 0.20) and  # 核心1：中期动量极强 (半年涨25%或3个月涨20%)
                (ma20 > ma50 and ma50 > ma200) and         # 核心2：均线呈完美多头排列 (证明是市场绝对龙头)
                (close > ma50) and                         # 核心3：死守50日生命线 (大趋势未破坏)
                (rsi <= 50) and                            # 核心4：RSI回落到中性或超卖区 (提供绝佳盈亏比买点)
                (dist_high >= -0.15)                       # 核心5：距最高点回撤不超15% (横盘代替下跌的强势调整)
            )

            # 如果均不满足，直接过滤
            if not (is_breakout or is_pullback or is_momentum_dip): continue

            # 结构标签识别 (优先级：动量回调 > 老龙回头 > 突破)
            if is_momentum_dip:
                struct_label = "⚡ 动量龙头回调 (RSI低位)"
            elif is_pullback:
                struct_label = "🐉 老龙回头 (靠近50日线)"
            else:
                struct_label = "🚀 强势突破"

            # 暂存通过技术面筛选的标的
            tech_passed_stocks.append({
                "Ticker": ticker,
                "Price": round(close, 2),
                "90D_Return%": round(ret_90d * 100, 2), 
                "120D_Return%": round(ret_120d * 100, 2),
                "RSI": round(rsi, 2),
                "Vol_Ratio": round(vol_ratio, 2),
                "Dist_High%": f"{round(dist_high * 100, 2)}%",
                "Turnover(M)": round((close * volume) / 1000000, 2),
                "Struct": struct_label
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
                stock["90D_Return%"] = f"{stock['90D_Return%']}%"
                stock["120D_Return%"] = f"{stock['120D_Return%']}%"
                final_stocks.append(stock)
                print(f"🎯 战术锁定: {stock['Ticker']} [{stock['Struct']}] | 市值: {round(market_cap_b,1)}B")
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
            df['Sort_Num'] = df['90D_Return%'].str.replace('%', '').astype(float)
            df = df.sort_values(by=['Struct', 'Sort_Num'], ascending=[False, False]).drop(columns=['Sort_Num'])
            
            cols = ["Ticker", "Struct", "Market_Cap(B)", "Price", "90D_Return%", "120D_Return%", "Dist_High%", "Vol_Ratio", "RSI", "Turnover(M)"]
            df = df[cols]
            
            data_to_write = [df.columns.values.tolist()] + df.values.tolist()
            sheet.clear()
            sheet.update(values=data_to_write, range_name="A1")
            
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("L1", "Last Updated:")
            sheet.update_acell("M1", now_time)
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
