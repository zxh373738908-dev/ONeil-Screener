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
# 2. 筹码峰 (Volume Profile) 核心算法
# ==========================================
def calculate_poc(df, bins=50):
    """
    计算过去 120 天的 POC (成交量最密集的价格点)
    """
    if len(df) < 60: return 0, 0
    
    # 取最近半年数据进行筹码分析
    lookback_df = df.tail(120)
    p_min = lookback_df['Low'].min()
    p_max = lookback_df['High'].max()
    
    if p_max == p_min: return 0, 0

    # 使用直方图计算价格区间成交量分布 (类似 TV 的 Volume Profile)
    counts, bin_edges = np.histogram(lookback_df['Close'], bins=bins, weights=lookback_df['Volume'])
    
    # 找到成交量最大的 bin 索引
    max_idx = np.argmax(counts)
    # 计算该区间的中心价格
    poc_price = (bin_edges[max_idx] + bin_edges[max_idx+1]) / 2
    
    current_price = df['Close'].iloc[-1]
    dist_to_poc = (current_price - poc_price) / poc_price
    
    return round(poc_price, 2), dist_to_poc

# ==========================================
# 3. [美股] 动量+筹码峰 专属扫描器
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
            
            # --- 筹码峰计算 ---
            poc_price, dist_to_poc = calculate_poc(df)
            
            # --- 核心指标计算 ---
            avg_vol_50 = df['Volume'].tail(50).mean()
            vol_ratio = volume / avg_vol_50 if avg_vol_50 > 0 else 0
            
            ma50 = df['Close'].tail(50).mean()
            ma200 = df['Close'].tail(200).mean()
            high_250 = df['High'].tail(250).max()
            
            dist_high = (close - high_250) / high_250
            dist_50ma = (close - ma50) / ma50
            
            # 周期涨幅 (20, 60, 90, 120天)
            ret_20d = (close - float(df['Close'].iloc[-21])) / float(df['Close'].iloc[-21])
            ret_60d = (close - float(df['Close'].iloc[-63])) / float(df['Close'].iloc[-63])
            ret_120d = (close - float(df['Close'].iloc[-126])) / float(df['Close'].iloc[-126])
            
            # 动量加权评分 (0.2*20 + 0.4*60 + 0.4*120)
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
            # ⚡ 战术 3：动量龙头回调 + 筹码支撑 (Momentum Dip with POC)
            # ====================================================
            # 筹码确认逻辑：价格必须在筹码峰之上 0-8% (允许洗盘小幅刺穿1%)
            is_on_poc_support = (dist_to_poc >= -0.01) and (dist_to_poc <= 0.08)

            is_momentum_dip = (
                (ret_120d >= 0.20 or ret_60d >= 0.15) and  # 中期动量强劲
                (ma50 > ma200) and                         # 长线多头
                (close >= ma50 * 0.95) and                 # 靠近50日线
                is_on_poc_support and                      # 🎯 核心新增：筹码峰支撑确认
                (rsi <= 58) and                            # RSI回调
                (dist_high >= -0.22)                       # 距高点回撤正常
            )

            if not is_momentum_dip: continue

            tech_passed_stocks.append({
                "Ticker": ticker,
                "Mom_Score": round(mom_score * 100, 2),
                "Price": round(close, 2),
                "POC_Price": poc_price,                    # 记录筹码支撑价
                "Dist_POC%": f"{round(dist_to_poc * 100, 2)}%", # 距离筹码峰距离
                "RSI": round(rsi, 2),
                "Dist_50MA%": f"{round(dist_50ma * 100, 2)}%",
                "Dist_High%": f"{round(dist_high * 100, 2)}%",
                "Vol_Ratio": round(vol_ratio, 2),
                "Turnover(M)": round((close * volume) / 1000000, 2)
            })
            
        except Exception:
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
                print(f"🎯 战术锁定: {stock['Ticker']} | 评分: {stock['Mom_Score']} | 距筹码峰: {stock['Dist_POC%']}")
        except:
            continue
            
    return final_stocks

def write_to_sheet(sheet_name, final_stocks):
    print("\n========== [3/3] 开始同步至指挥中心 (Google Sheets) ==========")
    try:
        sheet = client.open_by_url(SHEET_URL).worksheet(sheet_name)
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            df = df.sort_values(by=['Mom_Score'], ascending=[False])
            
            # 重新组织列顺序，突出筹码峰数据
            cols = [
                "Ticker", "Mom_Score", "Price", "POC_Price", "Dist_POC%",
                "RSI", "Dist_50MA%", "Dist_High%", "Market_Cap(B)",
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
            msg = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 无符合筹码支撑条件的股票。"
            sheet.update_acell("A1", msg)
            print(f"⚠️ 未发现符合条件的股票。")
    except Exception as e:
        print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    us_results = screen_us_stocks()
    write_to_sheet("Screener", us_results)
