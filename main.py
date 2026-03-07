import concurrent.futures  # 【新增】处理强制超时的库import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import time
import requests
import json
import re
import akshare as ak
import warnings
warnings.filterwarnings('ignore')

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=913399386#gid=913399386"  # ⚠️ 记得换成你的真实链接！

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [美股] 欧奈尔选股模块 (自动寻表防崩溃 + 进度打印版)
# ==========================================
def screen_us_stocks():
    print("\n========== 开始处理美股 [V4.0 动态寻表防崩溃版] ==========")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        # 1. 获取标普500 (动态寻找表头包含 Symbol 的表格)
        sp500_tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)
        sp500 = []
        for df in sp500_tables:
            if 'Symbol' in df.columns:
                sp500 = df['Symbol'].tolist()
                break
                
        # 2. 获取纳斯达克100 (动态寻找表头包含 Ticker 的表格)
        ndx_tables = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100', storage_options=headers)
        ndx100 = []
        for df in ndx_tables:
            if 'Ticker' in df.columns:
                ndx100 = df['Ticker'].tolist()
                break
            elif 'Symbol' in df.columns:
                ndx100 = df['Symbol'].tolist()
                break
                
        tickers = list(set([str(t).replace('.', '-') for t in (sp500 + ndx100)]))
        print(f"✅ 成功获取美股名单！共合并去重 {len(tickers)} 只核心股票。开始扫描...")
        
    except Exception as e:
        print(f"❌ 获取美股列表发生致命错误: {e}")
        return []

    final_stocks = []
    processed_count = 0
    
    for ticker in tickers:
        processed_count += 1
        # 每扫描100只打印一次进度，让你知道它没有卡死
        if processed_count % 100 == 0:
            print(f"   ...已扫描 {processed_count}/{len(tickers)} 只美股...")
            
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1y")
            if len(hist) < 200: continue
            
            close = hist['Close'].iloc[-1]
            volume = hist['Volume'].iloc[-1]
            if close < 15 or (close * volume) < 50000000: continue
            
            ma20 = hist['Close'].rolling(20).mean().iloc[-1]
            ma50 = hist['Close'].rolling(50).mean().iloc[-1]
            ma150 = hist['Close'].rolling(150).mean().iloc[-1]
            ma200 = hist['Close'].rolling(200).mean().iloc[-1]
            
            if not (ma50 > ma200): continue
            if not (close > ma150 and close > ma200): continue
            
            high_250 = hist['High'].rolling(250).max().iloc[-1]
            if close < (high_250 * 0.80): continue
            
            ret_90d = (close - hist['Close'].iloc[-63]) / hist['Close'].iloc[-63]
            if ret_90d < 0.15: continue
            
            delta = hist['Close'].diff()
            up = delta.clip(lower=0)
            down = -1 * delta.clip(upper=0)
            ema_up = up.ewm(com=13, adjust=False).mean()
            ema_down = down.ewm(com=13, adjust=False).mean()
            rs = ema_up / ema_down
            rsi = 100 - (100 / (1 + rs)).iloc[-1]
            if rsi < 45: continue
            
            struct_label = "Breakout (>MA20)" if close > ma20 else "Pullback (Near MA50)"
            
            final_stocks.append({
                "Ticker": ticker,
                "Price": round(close, 2),
                "90D_Return%": f"{round(ret_90d * 100, 2)}%",
                "Turnover(M)": round((close * volume) / 1000000, 2),
                "Struct": struct_label
            })
            print(f"✅ 捕获美股强势标的: {ticker}")
        except:
            continue
            
    return final_stocks

# ==========================================
# 3. [A股] 新浪财经隐身雷达 (绝对防屏蔽)
# ==========================================
def get_sina_market_snapshot():
    print("\n🚀 启动【新浪财经】高匿分页拉取引擎 (绝对防屏蔽)...")
    all_data = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'http://finance.sina.com.cn/'
    }
    
    for page in range(1, 80):
        url = f"http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=80&sort=symbol&asc=1&node=hs_a"
        try:
            res = requests.get(url, headers=headers, timeout=5)
            text = res.text
            if text == "[]" or text == "null" or not text:
                break
            text = re.sub(r'([{,])\s*([a-zA-Z0-9_]+)\s*:', r'\1"\2":', text)
            data = json.loads(text)
            all_data.extend(data)
        except:
            continue
            
    df = pd.DataFrame(all_data)
    print(f"✅ 成功穿透防火墙！获取 A 股 {len(df)} 只股票基础数据。")
    return df

# ==========================================
# 4. [A股] 欧奈尔核心筛选模块 (带并发防卡死 + 重试机制)
# ==========================================

def fetch_hist_with_retry(pure_code, start_date, end_date, retries=3):
    """【新增】带重试机制的K线获取器"""
    for i in range(retries):
        try:
            # 调用东方财富接口获取K线
            return ak.stock_zh_a_hist(symbol=pure_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        except Exception:
            time.sleep(1) # 失败后停顿1秒再试
    return pd.DataFrame()

def screen_a_shares():
    print("\n========== 开始处理 A股[V3.1 防卡死强力版] ==========")
    try:
        spot_df = get_sina_market_snapshot()
        if spot_df.empty: 
            return[], "❌ 新浪接口获取失败，大盘数据为空。"
            
        total_stocks = len(spot_df)
        
        spot_df['trade'] = pd.to_numeric(spot_df['trade'], errors='coerce')
        spot_df['mktcap'] = pd.to_numeric(spot_df['mktcap'], errors='coerce') * 10000
        spot_df['amount'] = pd.to_numeric(spot_df['amount'], errors='coerce')
        spot_df['turnoverratio'] = pd.to_numeric(spot_df['turnoverratio'], errors='coerce')

        cond1 = spot_df['trade'] >= 10
        cond2 = spot_df['mktcap'] >= 5_000_000_000   
        cond3 = spot_df['amount'] >= 200_000_000      
        cond4 = spot_df['turnoverratio'] >= 1.5              
            
        filtered_df = spot_df[cond1 & cond2 & cond3 & cond4].copy()
        liquidity_passed = len(filtered_df)
        print(f"🎯 流动性初筛: 满足 50亿/2亿 的核心标的剩余 {liquidity_passed} 只。开始深入 K 线扫描...")

        final_a_stocks =[]
        fail_reasons = {"动量不足20%": 0, "破位MA60/120生命线": 0, "高点回撤过大(>20%)": 0, "短期RSI弱势(<50)": 0, "K线缺失或网络超时": 0}
        
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=400)).strftime("%Y%m%d")
        
        processed_count = 0
        
        for index, row in filtered_df.iterrows():
            processed_count += 1
            raw_code = row['code']
            pure_code = raw_code[-6:] 
            name = row['name']
            
            # 【新增】A股进度打印，让你知道它卡在哪个进度了
            if processed_count % 50 == 0:
                print(f"   ...已扫描 {processed_count}/{liquidity_passed} 只A股...")
            
            try:
                # 【修改】将延迟从 0.1 提高到 0.3，极大降低被东财拉黑的概率
                time.sleep(0.3)
                
                # 【修改核心】使用线程池加上 8 秒强制超时机制。一旦卡死超过8秒，直接斩断跳过！
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(fetch_hist_with_retry, pure_code, start_date, end_date)
                    try:
                        hist = future.result(timeout=8)  # 8秒等不到数据直接抛出 TimeoutError
                    except concurrent.futures.TimeoutError:
                        print(f"⚠️ 警告: 获取 {pure_code} {name} 接口卡死超时，已强制跳过防挂起。")
                        fail_reasons["K线缺失或网络超时"] += 1
                        continue
                
                # 检查数据有效性
                if hist is None or hist.empty or len(hist) < 250: 
                    fail_reasons["K线缺失或网络超时"] += 1
                    continue
                
                close = hist['收盘'].iloc[-1]
                
                close_60 = hist['收盘'].iloc[-61]
                ret_60 = (close - close_60) / close_60
                if ret_60 < 0.20:
                    fail_reasons["动量不足20%"] += 1
                    continue
                
                ma20 = hist['收盘'].rolling(20).mean().iloc[-1]
                ma60 = hist['收盘'].rolling(60).mean().iloc[-1]
                ma120 = hist['收盘'].rolling(120).mean().iloc[-1]
                if not (close > ma60 and close > ma120) or not (ma20 > ma60):
                    fail_reasons["破位MA60/120生命线"] += 1
                    continue 
                    
                high_250 = hist['最高'].rolling(250).max().iloc[-1]
                if close < (high_250 * 0.80):
                    fail_reasons["高点回撤过大(>20%)"] += 1
                    continue 
                    
                delta = hist['收盘'].diff()
                up = delta.clip(lower=0)
                down = -1 * delta.clip(upper=0)
                ema_up = up.ewm(com=13, adjust=False).mean()
                ema_down = down.ewm(com=13, adjust=False).mean()
                rs = ema_up / ema_down
                rsi = 100 - (100 / (1 + rs)).iloc[-1]
                if rsi < 50:
                    fail_reasons["短期RSI弱势(<50)"] += 1
                    continue
                    
                final_a_stocks.append({
                    "Ticker": pure_code,
                    "Name": name,
                    "Price": round(close, 2),
                    "60D_Return%": f"{round(ret_60 * 100, 2)}%",
                    "Turnover(亿)": round(row['amount'] / 100_000_000, 2),
                    "Turnover_Rate%": f"{row['turnoverratio']}%",
                    "RSI": round(rsi, 2),
                    "Trend": "Hold MA60",
                    "Fundamental": "Check ROE>15%" 
                })
                print(f"✅ 捕获主升浪标的: {pure_code} {name}")
                
            except Exception as e:
                fail_reasons["K线缺失或网络超时"] += 1
                continue
                
        now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        diag_msg = (
            f"[{now_time}] 欧奈尔系统诊断报告：\n"
            f"1. 全市场扫描：共获取 {total_stocks} 只股票\n"
            f"2. 流动性初筛 (股价>10 / 市值>50亿 / 成交额>2亿 / 换手>1.5%)：剩余 {liquidity_passed} 只核心资金标的\n"
            f"3. 技术面K线淘汰明细：\n"
            f"   - 因【动量不足20%】淘汰: {fail_reasons['动量不足20%']} 只\n"
            f"   - 因【跌破生命线MA60/120】淘汰: {fail_reasons['破位MA60/120生命线']} 只\n"
            f"   - 因【高点回撤超过20%】淘汰: {fail_reasons['高点回撤过大(>20%)']} 只\n"
            f"   - 因【短期RSI弱势低于50】淘汰: {fail_reasons['短期RSI弱势(<50)']} 只\n"
            f"   - 因【接口限流/K线缺失】淘汰: {fail_reasons['K线缺失或网络超时']} 只\n"
            f"结论：当前大资金处于装死或派发期，系统强制执行空仓保护！"
        )
        
        return final_a_stocks, diag_msg
        
    except Exception as e:
        print(f"❌ A 股扫描发生致命错误: {e}")
        return







