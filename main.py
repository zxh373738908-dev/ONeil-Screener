
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
SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"  # ⚠️ 记得换成你的真实链接！

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [美股] 欧奈尔选股模块 
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
# 3. [A股] 防阻断获取大盘数据核心逻辑
# ==========================================
def get_a_share_market_data():
    # 策略1：尝试直接获取全市场（重试3次）
    for attempt in range(3):
        try:
            print(f"📡 尝试获取 A 股全市场数据 (第 {attempt+1} 次)...")
            df = ak.stock_zh_a_spot_em()
            if not df.empty:
                print("✅ 全市场数据获取成功！")
                return df
        except Exception as e:
            print(f"⚠️ 第 {attempt+1} 次被防火墙阻断: {e}")
            time.sleep(2) # 停顿2秒再试
            
    # 策略2：如果全市场被彻底封杀，采用“分块拉取”策略（沪深分离，大幅降低单次请求体积）
    print("🚀 全市场接口受限，启动【拆包分块下载】策略绕过防火墙...")
    try:
        df_sh = ak.stock_sh_a_spot_em() # 仅拉取上海
        time.sleep(1)
        df_sz = ak.stock_sz_a_spot_em() # 仅拉取深圳
        df_all = pd.concat([df_sh, df_sz], ignore_index=True)
        print("✅ 分拆拉取成功！已重新拼接全市场数据。")
        return df_all
    except Exception as e:
        print(f"❌ 终极获取失败，API 完全瘫痪: {e}")
        return pd.DataFrame() # 返回空表

# ==========================================
# 4. [A股] 欧奈尔核心筛选模块
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股 (强力抗阻断版) ==========")
    try:
        spot_df = get_a_share_market_data()
        if spot_df.empty: return []
        
        # 强制转换数据类型
        spot_df['最新价'] = pd.to_numeric(spot_df['最新价'], errors='coerce')
        spot_df['总市值'] = pd.to_numeric(spot_df['总市值'], errors='coerce')
        spot_df['成交额'] = pd.to_numeric(spot_df['成交额'], errors='coerce')
        spot_df['换手率'] = pd.to_numeric(spot_df['换手率'], errors='coerce')
        
        # 处理 60日涨跌幅 可能不存在的情况 (兼容沪深分块数据)
        if '60日涨跌幅' in spot_df.columns:
            spot_df['60日涨跌幅'] = pd.to_numeric(spot_df['60日涨跌幅'], errors='coerce')
            has_momentum = True
        else:
            has_momentum = False

        # 核心初筛：市值 ≥ 150亿，股价 ≥ 10，日成交额 ≥ 5亿，换手率 ≥ 1.5%
        cond = (spot_df['最新价'] >= 10) & (spot_df['总市值'] >= 15_000_000_000) & (spot_df['成交额'] >= 500_000_000) & (spot_df['换手率'] >= 1.5)
        
        if has_momentum:
            cond = cond & (spot_df['60日涨跌幅'] >= 30)
            
        filtered_df = spot_df[cond].copy()
        print(f"初筛完成：满足顶级流动性的候选股剩 {len(filtered_df)} 只。开始 K 线形态扫描...")

        final_a_stocks = []
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=400)).strftime("%Y%m%d")
        
        for index, row in filtered_df.iterrows():
            code = row['代码']
            name = row['名称']
            try:
                # 停顿 0.3 秒，防止查询 K 线时再次被拉黑
                time.sleep(0.3)
                hist = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                if len(hist) < 250: continue
                    
                close = hist['收盘'].iloc[-1]
                
                # 如果没用上 60日接口，手动测算动量
                if not has_momentum:
                    close_60 = hist['收盘'].iloc[-61]
                    ret_60 = (close - close_60) / close_60
                    if ret_60 < 0.30: continue
                    momentum_60d = ret_60 * 100
                else:
                    momentum_60d = row['60日涨跌幅']
                
                # 均线多头测算
                ma20 = hist['收盘'].rolling(20).mean().iloc[-1]
                ma60 = hist['收盘'].rolling(60).mean().iloc[-1]
                ma120 = hist['收盘'].rolling(120).mean().iloc[-1]
                if not (close > ma20 and close > ma60 and close > ma120): continue
                if not (ma20 > ma60): continue
                    
                # 突破历史新高测算 (250日最高价 85%)
                high_250 = hist['最高'].rolling(250).max().iloc[-1]
                if close < (high_250 * 0.85): continue
                    
                # RSI 测算
                delta = hist['收盘'].diff()
                up = delta.clip(lower=0)
                down = -1 * delta.clip(upper=0)
                ema_up = up.ewm(com=13, adjust=False).mean()
                ema_down = down.ewm(com=13, adjust=False).mean()
                rs = ema_up / ema_down
                rsi = 100 - (100 / (1 + rs)).iloc[-1]
                if rsi < 55: continue
                    
                # ================= 白名单 =================
                final_a_stocks.append({
                    "Ticker": code,
                    "Name": name,
                    "Price": round(close, 2),
                    "60D_Return%": f"{round(momentum_60d, 2)}%",
                    "Turnover(亿)": round(row['成交额'] / 100_000_000, 2),
                    "Turnover_Rate%": f"{row['换手率']}%",
                    "RSI": round(rsi, 2),
                    "Trend": "MA20>60>120",
                    "Fundamental": "Check ROE>15%" 
                })
                print(f"🎯 捕获主升浪: {code} {name} (60日涨幅 {round(momentum_60d, 2)}%)")
                
            except Exception as e:
                continue
                
        return final_a_stocks
        
    except Exception as e:
        print(f"A 股扫描发生致命错误: {e}")
        return []

# ==========================================
# 5. 写入 Google Sheets
# ==========================================
def write_to_sheet(sheet_name, final_stocks, sort_col):
    try:
        sheet = client.open_by_url(SHEET_URL).worksheet(sheet_name)
        if final_stocks:
            df = pd.DataFrame(final_stocks)
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
            sheet.update_acell("A1", f"[{now_time}] 当下大盘无个股同时满足：150亿市值、5亿成交额、60日涨幅超30% 且逼近历史新高。")
            print(f"⚠️ {sheet_name}: 无符合条件的股票。")
    except Exception as e:
        print(f"❌ 写入 {sheet_name} 失败: {e}")

# ==========================================
# 6. 主程序启动
# ==========================================
if __name__ == "__main__":
    us_results = screen_us_stocks()
    write_to_sheet("Screener", us_results, sort_col="90D_Return%")
    
    a_results = screen_a_shares()
    write_to_sheet("A-Share Screener", a_results, sort_col="60D_Return%")
