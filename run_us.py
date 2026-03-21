import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. Google Sheets 连接配置
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"  
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [核心引擎] 筹码分布 (Volume Profile) 计算函数
# ==========================================
def get_volume_profile_poc(df, bins=50):
    """
    模拟 TradingView 的筹码峰计算
    返回: (POC价格, 距离POC的百分比)
    """
    if len(df) < 60: return 0, 0
    
    # 取最近 120 天的筹码分布（约半年，机构主要建仓周期）
    analysis_df = df.tail(120)
    v_min, v_max = analysis_df['Low'].min(), analysis_df['High'].max()
    
    # 建立价格区间桶
    counts, bins_edges = np.histogram(analysis_df['Close'], bins=bins, weights=analysis_df['VolumeRange'])
    
    # 找到成交量最大的价格区间索引
    max_idx = np.argmax(counts)
    poc_price = (bins_edges[max_idx] + bins_edges[max_idx+1]) / 2
    
    current_price = df['Close'].iloc[-1]
    dist_to_poc = (current_price - poc_price) / poc_price
    
    return round(poc_price, 2), dist_to_poc

# ==========================================
# 3. [美股] 动量+筹码 专属扫描器
# ==========================================
def get_us_tickers():
    print("\n========== [1/3] 获取美股核心目标群 (S&P500 + NDX100) ==========")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].tolist()
        ndx100 = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100', storage_options=headers)[4]['Ticker'].tolist()
        tickers = list(set([str(t).replace('.', '-') for t in (sp500 + ndx100)]))
        print(f"✅ 获取成功，共计 {len(tickers)} 只核心股票。")
        return tickers
    except Exception as e:
        print(f"❌ 获取列表失败: {e}")
        return []

def screen_us_stocks():
    tickers = get_us_tickers()
    if not tickers: return []
    
    print("\n========== [2/3] 正在下载数据并计算筹码分布... ==========")
    # 批量下载，包含 High/Low 用于计算筹码分布
    data = yf.download(tickers, period="1y", group_by='ticker', threads=True, progress=False)

    final_results = []
    
    for ticker in tickers:
        try:
            if ticker not in data.columns.levels[0]: continue
            df = data[ticker].dropna()
            if len(df) < 200: continue
            
            # 基础数据准备
            close = float(df['Close'].iloc[-1])
            volume = float(df['Volume'].iloc[-1])
            avg_vol_50 = df['Volume'].tail(50).mean()
            
            # 流动性初筛
            if close < 15 or (close * volume) < 50000000: continue
            
            # --- 筹码分布计算 ---
            # 使用 Close 作为简化成交量权重分配，模拟 TV 的 Volume Profile
            df['VolumeRange'] = df['Volume'] 
            poc_price, dist_to_poc = get_volume_profile_poc(df)
            
            # --- 动量指标逻辑 (复刻 0.2/0.4/0.4) ---
            ret_20d = (close - df['Close'].iloc[-21]) / df['Close'].iloc[-21]
            ret_60d = (close - df['Close'].iloc[-63]) / df['Close'].iloc[-63]
            ret_120d = (close - df['Close'].iloc[-126]) / df['Close'].iloc[-126]
            mom_score = (0.2 * ret_20d) + (0.4 * ret_60d) + (0.4 * ret_120d)
            
            # --- 其他辅助指标 ---
            ma50 = df['Close'].tail(50).mean()
            ma200 = df['Close'].tail(200).mean()
            high_250 = df['High'].tail(250).max()
            
            # RSI 计算
            delta = df['Close'].tail(20).diff()
            up, down = delta.copy(), delta.copy()
            up[up < 0] = 0
            down[down > 0] = 0
            rsi = 100 - (100 / (1 + (up.ewm(com=13).mean() / -down.ewm(com=13).mean()).iloc[-1]))

            # ====================================================
            # ⚡ 战术逻辑：[动量老龙] + [筹码支撑区回调]
            # ====================================================
            is_momentum = (ret_120d >= 0.20 or ret_60d >= 0.15) and (ma50 > ma200)
            
            # 筹码过滤：当前价格在 POC 上方 0%-6% 范围内 (最强支撑买点)
            # 且距离高点回撤不超过 20%
            is_chip_support = (dist_to_poc >= -0.01) and (dist_to_poc <= 0.06)
            
            if is_momentum and is_chip_support and (rsi <= 60):
                # 基本面：市值核查
                ticker_obj = yf.Ticker(ticker)
                mkt_cap_b = ticker_obj.info.get('marketCap', 0) / 1_000_000_000
                
                if mkt_cap_b >= 2.0:
                    final_results.append({
                        "Ticker": ticker,
                        "Score": round(mom_score * 100, 2), # 综合动量评分
                        "Price": round(close, 2),
                        "POC_Support": poc_price,           # TV 里的筹码红线
                        "Dist_POC%": f"{round(dist_to_poc * 100, 2)}%", # 距离筹码支撑有多远
                        "RSI": round(rsi, 2),
                        "MarketCap(B)": round(mkt_cap_b, 2),
                        "Dist_High%": f"{round(((close-high_250)/high_250)*100, 2)}%",
                        "60D_Ret%": f"{round(ret_60d * 100, 2)}%",
                        "Vol_Ratio": round(volume / avg_vol_50, 2)
                    })
                    print(f"🎯 筹码锁定: {ticker} | 支撑位: {poc_price} | 动量分: {round(mom_score*100,2)}")

        except Exception as e:
            continue
            
    return final_results

# ==========================================
# 4. 指挥中心：写入 Google Sheets
# ==========================================
def write_to_sheet(sheet_name, final_stocks):
    print("\n========== [3/3] 同步至 Google Sheets 筹码监控板 ==========")
    try:
        sheet = client.open_by_url(SHEET_URL).worksheet(sheet_name)
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            # 优先按动量评分排序
            df = df.sort_values(by=['Score'], ascending=False)
            
            # 定义列顺序
            cols = ["Ticker", "Score", "Price", "POC_Support", "Dist_POC%", "RSI", "MarketCap(B)", "Dist_High%", "60D_Ret%", "Vol_Ratio"]
            df = df[cols]
            
            data_to_write = [df.columns.values.tolist()] + df.values.tolist()
            sheet.clear()
            sheet.update(values=data_to_write, range_name="A1")
            
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("L1", "Last Updated:")
            sheet.update_acell("M1", now_time)
            print(f"🎉 任务完成！共计 {len(df)} 只标的已进入筹码监控区。")
        else:
            sheet.clear()
            sheet.update_acell("A1", f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 市场无符合筹码回调标的。")
            print("⚠️ 未发现符合条件的标的。")
    except Exception as e:
        print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    us_results = screen_us_stocks()
    write_to_sheet("Screener", us_results)
