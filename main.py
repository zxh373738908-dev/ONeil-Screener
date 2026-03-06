import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import akshare as ak

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
SHEET_URL = "你的GOOGLE表格链接填在这里" # ⚠️ 记得换成你的真实链接！

scopes =["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [美股] 欧奈尔选股模块 (保留你原有的美股逻辑)
# ==========================================
def screen_us_stocks():
    print("\n========== 开始处理美股 ==========")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)[0]['Symbol'].tolist()
        ndx100 = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100', storage_options=headers)[4]['Ticker'].tolist()
        tickers = list(set([t.replace('.', '-') for t in (sp500 + ndx100)]))
    except Exception as e:
        print("获取美股列表失败")
        return[]

    final_stocks =[]
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1y")
            if len(hist) < 200: continue
            
            close = hist['Close'].iloc[-1]
            volume = hist['Volume'].iloc[-1]
            if close < 15 or (close * volume) < 50000000: continue
            
            ma20 = hist['Close'].rolling(20).mean().iloc[-1]
            ma50 = hist['Close'].rolling(50).mean().iloc[-1]
            ma200 = hist['Close'].rolling(200).mean().iloc[-1]
            if not (close > ma20 and ma20 > ma50 and close > ma200): continue
            
            high_250 = hist['High'].rolling(250).max().iloc[-1]
            if close < (high_250 * 0.85): continue
            
            ret_90d = (close - hist['Close'].iloc[-63]) / hist['Close'].iloc[-63]
            if ret_90d < 0.25: continue
            
            final_stocks.append({
                "Ticker": ticker,
                "Price": round(close, 2),
                "90D_Return%": f"{round(ret_90d * 100, 2)}%",
                "Turnover(M)": round((close * volume) / 1000000, 2),
                "Struct": "MA20>MA50"
            })
        except:
            continue
    return final_stocks

# ==========================================
# 3. [A股] 机构主升浪终极过滤模块 (对接腾讯/东方财富全市场数据)
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股 (底层对接全市场数据) ==========")
    try:
        # 1. 获取全市场 A 股实时行情 (包含最新价、市值、换手率等)
        spot_df = ak.stock_zh_a_spot_em()
        
        # 数据类型清洗
        spot_df['最新价'] = pd.to_numeric(spot_df['最新价'], errors='coerce')
        spot_df['总市值'] = pd.to_numeric(spot_df['总市值'], errors='coerce')
        spot_df['成交额'] = pd.to_numeric(spot_df['成交额'], errors='coerce')
        spot_df['换手率'] = pd.to_numeric(spot_df['换手率'], errors='coerce')
        
        # 2. 严格执行市值与流动性过滤
        # 总市值 ≥ 150亿, 股价 ≥ 10, 日成交额 ≥ 5亿(500,000,000), 换手率 ≥ 1.5%
        cond1 = spot_df['最新价'] >= 10
        cond2 = spot_df['总市值'] >= 150_000_000_000  # 接口单位为元
        cond3 = spot_df['成交额'] >= 500_000_000
        cond4 = spot_df['换手率'] >= 1.5
        
        filtered_df = spot_df[cond1 & cond2 & cond3 & cond4]
        tickers = filtered_df['代码'].tolist()
        names = filtered_df['名称'].tolist()
        
        print(f"第一轮基础过滤完成：全市场 5000 多只股票中，仅剩 {len(tickers)} 只满足顶级流动性要求。开始扫描技术面...")
        
        final_a_stocks =[]
        # 准备时间范围 (抓取过去400天数据，确保有足够的 250 个交易日)
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=400)).strftime("%Y%m%d")
        
        for i, code in enumerate(tickers):
            try:
                # 获取该股票的日K线历史 (前复权)
                hist = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                if len(hist) < 250:
                    continue
                    
                close = hist['收盘'].iloc[-1]
                
                # 3. 趋势结构测算
                ma20 = hist['收盘'].rolling(20).mean().iloc[-1]
                ma60 = hist['收盘'].rolling(60).mean().iloc[-1]
                ma120 = hist['收盘'].rolling(120).mean().iloc[-1]
                
                # 收盘价 > MA20, MA60, MA120 且 MA20 > MA60
                if not (close > ma20 and close > ma60 and close > ma120): continue
                if not (ma20 > ma60): continue
                    
                # 4. 突破位置测算 (近250日最高价的 85% 以上)
                high_250 = hist['最高'].rolling(250).max().iloc[-1]
                if close < (high_250 * 0.85): continue
                    
                # 5. 动量测算 (60日涨幅 >= 30%)
                close_60 = hist['收盘'].iloc[-61]
                ret_60 = (close - close_60) / close_60
                if ret_60 < 0.30: continue
                    
                # 6. RSI (14) >= 55 测算
                delta = hist['收盘'].diff()
                up = delta.clip(lower=0)
                down = -1 * delta.clip(upper=0)
                ema_up = up.ewm(com=13, adjust=False).mean()
                ema_down = down.ewm(com=13, adjust=False).mean()
                rs = ema_up / ema_down
                rsi = 100 - (100 / (1 + rs)).iloc[-1]
                if rsi < 55: continue
                    
                # ================= 满足所有严苛条件，加入白名单 =================
                name = names[i]
                turnover_rate = filtered_df.iloc[i]['换手率']
                turnover_amt = filtered_df.iloc[i]['成交额']
                
                ret_1d = (close - hist['收盘'].iloc[-2]) / hist['收盘'].iloc[-2]
                
                final_a_stocks.append({
                    "Ticker": code,
                    "Name": name,
                    "Price": round(close, 2),
                    "1D%": f"{round(ret_1d * 100, 2)}%",
                    "60D_Return%": f"{round(ret_60 * 100, 2)}%",
                    "Turnover(亿)": round(turnover_amt / 100_000_000, 2),
                    "Turnover_Rate%": f"{turnover_rate}%",
                    "RSI": round(rsi, 2),
                    "Struct": "MA20>MA60>MA120",
                    "Fundamentals": "Check (ROE>15%, Growth>20%)" # 提醒看基本面
                })
                print(f"✅ 捕获 A 股主升浪标的: {code} {name}")
                
            except Exception as e:
                continue
                
        return final_a_stocks
        
    except Exception as e:
        print(f"A 股扫描发生致命错误: {e}")
        return[]

# ==========================================
# 4. 写入 Google Sheets
# ==========================================
def write_to_sheet(sheet_name, final_stocks, sort_col):
    try:
        sheet = client.open_by_url(SHEET_URL).worksheet(sheet_name)
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            # 排序 (美股按90日涨幅，A股按60日涨幅降序，抓最强动量)
            df['Sort_Num'] = df[sort_col].str.replace('%', '').astype(float)
            df = df.sort_values(by='Sort_Num', ascending=False).drop(columns=['Sort_Num'])
            
            data_to_write = [df.columns.values.tolist()] + df.values.tolist()
            sheet.clear()
            sheet.update(values=data_to_write, range_name="A1")
            
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_cell(1, len(df.columns) + 2, "Last Updated:")
            sheet.update_cell(1, len(df.columns) + 3, now_time)
            print(f"🎉 成功将 {len(df)} 只最强股票写入 {sheet_name}！")
        else:
            sheet.clear()
            sheet.update_acell("A1", "今天大盘较弱，没有完全符合条件的欧奈尔突破股。")
            print(f"⚠️ {sheet_name}: 今天无符合条件的股票。")
    except Exception as e:
        print(f"❌ 写入 {sheet_name} 失败: {e}")

# ==========================================
# 5. 主程序启动
# ==========================================
if __name__ == "__main__":
    # 执行美股
    us_results = screen_us_stocks()
    write_to_sheet("Screener", us_results, sort_col="90D_Return%")
    
    # 执行A股
    a_results = screen_a_shares()
    write_to_sheet("A-Share Screener", a_results, sort_col="60D_Return%")
