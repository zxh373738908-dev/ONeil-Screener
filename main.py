import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import akshare as ak
import time

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=913399386#gid=913399386"  # ⚠️ 记得换成你的真实链接！

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [美股] 欧奈尔选股模块 (保持原样，运行成功)
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
        return []

    final_stocks = []
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
# 3. [A股] 腾讯数据源防封杀模块 (双通道智能切换)
# ==========================================
def fetch_a_share_snapshot():
    # 通道 1：尝试获取自带涨跌幅的接口（可能被阻断）
    try:
        print("尝试通道 1：获取全市场实时数据...")
        df = ak.stock_zh_a_spot_em()
        df['最新价'] = pd.to_numeric(df['最新价'], errors='coerce')
        df['总市值'] = pd.to_numeric(df['总市值'], errors='coerce')
        df['成交额'] = pd.to_numeric(df['成交额'], errors='coerce')
        df['换手率'] = pd.to_numeric(df['换手率'], errors='coerce')
        df['60日涨跌幅'] = pd.to_numeric(df['60日涨跌幅'], errors='coerce')
        
        # 150亿市值(15,000,000,000) / 10元 / 5亿成交额 / 1.5%换手 / 30%涨幅
        cond = (df['最新价'] >= 10) & (df['总市值'] >= 15_000_000_000) & (df['成交额'] >= 500_000_000) & (df['换手率'] >= 1.5) & (df['60日涨跌幅'] >= 30)
        filtered = df[cond].copy()
        print(f"通道 1 获取成功，初筛剩余 {len(filtered)} 只股票。")
        return filtered, True
        
    except Exception as e:
        print(f"\n⚠️ 通道 1 遭到防火墙阻断: {e}")
        print("🚀 立即切换至【腾讯财经 (Tencent)】备用安全通道...")
        
        # 通道 2：腾讯财经安全接口（100% 不会被封，但需要在后续手动算 60 日涨幅）
        df_tx = ak.stock_zh_a_spot_tx() # 腾讯接口
        df_tx['最新价'] = pd.to_numeric(df_tx['最新价'], errors='coerce')
        df_tx['总市值'] = pd.to_numeric(df_tx['总市值'], errors='coerce')
        df_tx['成交额'] = pd.to_numeric(df_tx['成交额'], errors='coerce')
        df_tx['换手率'] = pd.to_numeric(df_tx['换手率'], errors='coerce')
        
        # 腾讯接口没有 60日涨跌幅 字段，所以先过滤基础流动性，缩小池子后再去算 K 线
        cond = (df_tx['最新价'] >= 10) & (df_tx['总市值'] >= 15_000_000_000) & (df_tx['成交额'] >= 500_000_000) & (df_tx['换手率'] >= 1.5)
        filtered = df_tx[cond].copy()
        print(f"✅ 腾讯通道获取成功，初筛高流动性大盘股剩余 {len(filtered)} 只股票。开始深度测算...")
        return filtered, False

def screen_a_shares():
    print("\n========== 开始处理 A股 (腾讯/双通道优化版) ==========")
    try:
        filtered_df, has_momentum_prefiltered = fetch_a_share_snapshot()
        
        if filtered_df.empty:
            print("市场冰点，无满足基础流动性的股票。")
            return []

        final_a_stocks = []
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=400)).strftime("%Y%m%d")
        
        count = 0
        for index, row in filtered_df.iterrows():
            code = row['代码']
            name = row['名称']
            try:
                # 获取日 K 线 (前复权) - 每次停顿 0.1 秒防止被拉黑
                time.sleep(0.1) 
                hist = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                if len(hist) < 250:
                    continue
                    
                close = hist['收盘'].iloc[-1]
                
                # 1. 如果用的是腾讯通道，必须手动补算 60 日涨幅 >= 30% 的条件
                if not has_momentum_prefiltered:
                    close_60 = hist['收盘'].iloc[-61]
                    ret_60 = (close - close_60) / close_60
                    if ret_60 < 0.30: continue
                    momentum_60d = ret_60 * 100
                else:
                    momentum_60d = row['60日涨跌幅']
                
                # 2. 均线结构测算 (严格按照要求)
                ma20 = hist['收盘'].rolling(20).mean().iloc[-1]
                ma60 = hist['收盘'].rolling(60).mean().iloc[-1]
                ma120 = hist['收盘'].rolling(120).mean().iloc[-1]
                
                # 收盘价 > 20/60/120 且 MA20 > MA60
                if not (close > ma20 and close > ma60 and close > ma120): continue
                if not (ma20 > ma60): continue
                    
                # 3. 欧奈尔突破位置 (当前价 >= 250日最高价的 85%)
                high_250 = hist['最高'].rolling(250).max().iloc[-1]
                if close < (high_250 * 0.85): continue
                    
                # 4. 动量 RSI (14) >= 55 测算
                delta = hist['收盘'].diff()
                up = delta.clip(lower=0)
                down = -1 * delta.clip(upper=0)
                ema_up = up.ewm(com=13, adjust=False).mean()
                ema_down = down.ewm(com=13, adjust=False).mean()
                rs = ema_up / ema_down
                rsi = 100 - (100 / (1 + rs)).iloc[-1]
                if rsi < 55: continue
                    
                # ================= 满足所有条件，加入核心池 =================
                final_a_stocks.append({
                    "Ticker": code,
                    "Name": name,
                    "Price": round(close, 2),
                    "60D_Return%": f"{round(momentum_60d, 2)}%",
                    "Turnover(亿)": round(row['成交额'] / 100_000_000, 2),
                    "Turnover_Rate%": f"{row['换手率']}%",
                    "RSI": round(rsi, 2),
                    "Trend": "Check OK",
                    "Fundamental": "Check ROE>15%" # 提醒看基本面
                })
                count += 1
                print(f"🎯 捕获 A 股主升浪: {code} {name} (60日涨幅 {round(momentum_60d, 2)}%)")
                
            except Exception as e:
                continue
                
        return final_a_stocks
        
    except Exception as e:
        print(f"A 股扫描发生致命错误: {e}")
        return []

# ==========================================
# 4. 写入 Google Sheets
# ==========================================
def write_to_sheet(sheet_name, final_stocks, sort_col):
    try:
        sheet = client.open_by_url(SHEET_URL).worksheet(sheet_name)
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            # 按动量(涨幅)降序排序，寻找最强领头羊
            df['Sort_Num'] = df[sort_col].str.replace('%', '').astype(float)
            df = df.sort_values(by='Sort_Num', ascending=False).drop(columns=['Sort_Num'])
            
            data_to_write = [df.columns.values.tolist()] + df.values.tolist()
            sheet.clear()
            sheet.update(values=data_to_write, range_name="A1")
            
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_cell(1, len(df.columns) + 2, "Last Updated:")
            sheet.update_cell(1, len(df.columns) + 3, now_time)
            print(f"🎉 成功将 {len(df)} 只最强龙头写入 {sheet_name}！")
        else:
            sheet.clear()
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("A1", f"[{now_time}] 当下大盘无个股同时满足：150亿市值、5亿成交额、60日涨幅超30% 且逼近历史新高。空仓等待！")
            print(f"⚠️ {sheet_name}: 无符合条件的股票。")
    except Exception as e:
        print(f"❌ 写入 {sheet_name} 失败: {e}")

# ==========================================
# 5. 主程序启动
# ==========================================
if __name__ == "__main__":
    us_results = screen_us_stocks()
    write_to_sheet("Screener", us_results, sort_col="90D_Return%")
    
    a_results = screen_a_shares()
    write_to_sheet("A-Share Screener", a_results, sort_col="60D_Return%")
