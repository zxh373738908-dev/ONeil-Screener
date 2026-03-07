import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import time
import requests
import json
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import warnings
warnings.filterwarnings('ignore')

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"  

scopes =["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# ==========================================
# 2. [美股] 欧奈尔选股 (去雅虎化，100% 东方财富极速版)
# ==========================================
def get_us_secid_map():
    """独家黑科技：获取全美股 Ticker 到东方财富内部 secid 的映射字典"""
    url = "https://82.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=20000&fs=m:105,m:106,m:107&fields=f12,f13"
    em_secid_map = {}
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10).json()
        items = res.get('data', {}).get('diff',[])
        for item in items:
            raw_ticker = str(item['f12'])
            # 构建类似 105.AAPL 的专属代码
            secid = f"{item['f13']}.{raw_ticker}"
            em_secid_map[raw_ticker] = secid
            em_secid_map[raw_ticker.replace('.', '-')] = secid
            em_secid_map[raw_ticker.replace('-', '.')] = secid
    except Exception as e:
        print(f"获取美股映射字典失败: {e}")
    return em_secid_map

def screen_us_stocks():
    print("\n========== 开始处理美股 [V6.1 去雅虎化极速版] ==========")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        sp_tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', storage_options=headers)
        sp500 = next((df['Symbol'].tolist() for df in sp_tables if 'Symbol' in df.columns),[])
        
        ndx_tables = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100', storage_options=headers)
        ndx100 =[]
        for df in ndx_tables:
            if 'Ticker' in df.columns: ndx100 = df['Ticker'].tolist(); break
            elif 'Symbol' in df.columns: ndx100 = df['Symbol'].tolist(); break
        
        tickers = list(set([str(t).replace('.', '-') for t in (sp500 + ndx100)]))
        print(f"✅ 成功获取美股名单！合并去重共 {len(tickers)} 只核心股票。")
    except Exception as e:
        print(f"❌ 获取美股列表失败: {e}")
        return[]

    em_secid_map = get_us_secid_map()
    if not em_secid_map:
        print("⚠️ 无法获取东方财富美股字典，扫描中断。")
        return []

    final_stocks =[]
    consecutive_fails = 0
    
    # 建立强力防抖连接池
    em_session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
    em_session.mount('https://', HTTPAdapter(max_retries=retries))
    em_session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    for ticker in tickers:
        secid = em_secid_map.get(ticker) or em_secid_map.get(ticker.replace('-', '.'))
        if not secid:
            continue
            
        try:
            # 同样使用 Push2His，彻底摆脱 yfinance 限制
            em_url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=1&end=20500101&lmt=300"
            res = em_session.get(em_url, timeout=5).json()
            
            if not res or 'data' not in res or not res['data'] or 'klines' not in res['data']:
                consecutive_fails += 1
                if consecutive_fails > 25:
                    print("\n⚠️ [警告] 美股连续获取失败，已触发熔断！")
                    break 
                continue
                
            klines = res['data']['klines']
            if len(klines) < 200:
                continue
                
            consecutive_fails = 0 
            
            closes = [float(k.split(',')[2]) for k in klines]
            highs = [float(k.split(',')[3]) for k in klines]
            vols = [float(k.split(',')[5]) for k in klines]
            amounts = [float(k.split(',')[6]) for k in klines] # f57 成交额直接返回美元计价
            
            close_series = pd.Series(closes)
            high_series = pd.Series(highs)
            
            close = close_series.iloc[-1]
            amount = amounts[-1]
            vol = vols[-1]
            
            if amount == 0: amount = close * vol
            if close < 15 or amount < 50000000: continue
            
            ma20 = close_series.rolling(20).mean().iloc[-1]
            ma50 = close_series.rolling(50).mean().iloc[-1]
            ma150 = close_series.rolling(150).mean().iloc[-1]
            ma200 = close_series.rolling(200).mean().iloc[-1]
            
            if not (ma50 > ma200) or not (close > ma150 and close > ma200): continue
            
            high_250 = high_series.rolling(250).max().iloc[-1]
            if close < (high_250 * 0.80): continue
            
            ret_90d = (close - close_series.iloc[-63]) / close_series.iloc[-63]
            if ret_90d < 0.15: continue
            
            delta = close_series.diff()
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
                "Turnover(M)": round(amount / 1000000, 2),
                "Struct": struct_label
            })
            print(f"✅ 捕获美股强势标的: {ticker}")
            
        except Exception as e:
            consecutive_fails += 1
            continue
            
    return final_stocks

# ==========================================
# 3.[A股] 新浪财经隐身雷达
# ==========================================
def get_sina_market_snapshot():
    print("\n🚀 启动【新浪财经】高匿分页拉取引擎...")
    all_data =[]
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'http://finance.sina.com.cn/'}
    
    for page in range(1, 80):
        url = f"http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=80&sort=symbol&asc=1&node=hs_a"
        try:
            res = requests.get(url, headers=headers, timeout=5)
            text = res.text
            if text == "[]" or text == "null" or not text: break
            text = re.sub(r'([{,])\s*([a-zA-Z0-9_]+)\s*:', r'\1"\2":', text)
            all_data.extend(json.loads(text))
        except:
            continue
    df = pd.DataFrame(all_data)
    print(f"✅ 获取 A 股 {len(df)} 只股票基础数据成功！")
    return df

# ==========================================
# 4. [A股] 欧奈尔核心筛选 (修复断点 Bug 版)
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股 [V6.1 修复时间戳断点版] ==========")
    try:
        spot_df = get_sina_market_snapshot()
        if spot_df.empty: 
            return[], "❌ 接口获取失败，大盘数据为空。"
            
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
        print(f"🎯 流动性初筛: 满足核心资金门槛剩余 {liquidity_passed} 只标的。")

        final_a_stocks =[]
        fail_reasons = {
            "动量不足20%": 0, 
            "破位MA60/120生命线": 0, 
            "高点回撤过大(>20%)": 0, 
            "短期RSI弱势(<50)": 0, 
            "次新股(不足250天)": 0,
            "接口拦截或退市": 0
        }
        
        consecutive_fails = 0
        
        em_session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
        em_session.mount('https://', HTTPAdapter(max_retries=retries))
        em_session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        
        for index, row in filtered_df.iterrows():
            raw_code = str(row['code'])
            pure_code = raw_code[-6:] 
            name = row['name']
            
            if pure_code.startswith(('6', '5')): prefix = "1"
            elif pure_code.startswith(('0', '3')): prefix = "0"
            else: continue

            try:
                secid = f"{prefix}.{pure_code}"
                # 【修复】修正了 end 日期为正确的 20500101，保证接口顺利拉取历史数据
                em_url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=1&end=20500101&lmt=300"
                
                res = em_session.get(em_url, timeout=5).json()
                
                if not res or 'data' not in res or not res['data'] or 'klines' not in res['data']:
                    fail_reasons["接口拦截或退市"] += 1
                    consecutive_fails += 1
                    continue
                
                consecutive_fails = 0 
                klines = res['data']['klines']
                
                if len(klines) < 250:
                    fail_reasons["次新股(不足250天)"] += 1
                    continue
                
                closes = [float(k.split(',')[2]) for k in klines]
                highs = [float(k.split(',')[3]) for k in klines]
                
                close_series = pd.Series(closes)
                high_series = pd.Series(highs)
                
                close = close_series.iloc[-1]
                close_60 = close_series.iloc[-61]
                
                ret_60 = (close - close_60) / close_60
                if ret_60 < 0.20:
                    fail_reasons["动量不足20%"] += 1
                    continue
                
                ma20 = close_series.rolling(20).mean().iloc[-1]
                ma60 = close_series.rolling(60).mean().iloc[-1]
                ma120 = close_series.rolling(120).mean().iloc[-1]
                if not (close > ma60 and close > ma120) or not (ma20 > ma60):
                    fail_reasons["破位MA60/120生命线"] += 1
                    continue 
                    
                high_250 = high_series.rolling(250).max().iloc[-1]
                if close < (high_250 * 0.80):
                    fail_reasons["高点回撤过大(>20%)"] += 1
                    continue 
                    
                delta = close_series.diff()
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
                fail_reasons["接口拦截或退市"] += 1
                consecutive_fails += 1
                if consecutive_fails > 30:
                    print("⚠️ 连续 30 只A股获取失败，触发熔断！")
                    break
                continue
                
        now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        diag_msg = (
            f"[{now_time}] 欧奈尔系统诊断报告：\n"
            f"1. 全市场扫描：共获取 {total_stocks} 只股票\n"
            f"2. 流动性初筛：剩余 {liquidity_passed} 只标的\n"
            f"3. 淘汰明细：\n"
            f"   - 【动量不足20%】: {fail_reasons['动量不足20%']} 只\n"
            f"   - 【跌破生命线】: {fail_reasons['破位MA60/120生命线']} 只\n"
            f"   - 【回撤超20%】: {fail_reasons['高点回撤过大(>20%)']} 只\n"
            f"   - 【RSI弱势】: {fail_reasons['短期RSI弱势(<50)']} 只\n"
            f"   - 【次新股不足天数】: {fail_reasons['次新股(不足250天)']} 只\n"
            f"   - 【网络错误/退市】: {fail_reasons['接口拦截或退市']} 只\n"
            f"结论：已完成最新一轮检测。"
        )
        return final_a_stocks, diag_msg
        
    except Exception as e:
        print(f"❌ A 股扫描致命错误: {e}")
        return[], "代码发生内部错误，请检查日志。"

# ==========================================
# 5. 写入 Google Sheets
# ==========================================
def write_to_sheet(sheet_name, final_stocks, sort_col, diag_msg=None):
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
            sheet.update_acell("I1", "Last Updated:")
            sheet.update_acell("J1", now_time)
            print(f"🎉 成功将 {len(df)} 只最强龙头写入 {sheet_name}！")
        else:
            sheet.clear()
            final_msg = diag_msg if diag_msg else f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 当下无符合条件的股票，或遇到极端行情空仓保护。"
            sheet.update_acell("A1", final_msg)
            print(f"⚠️ {sheet_name}: 已输出诊断报告/空仓警告。")
    except Exception as e:
        print(f"❌ 写入 {sheet_name} 失败: {e}")

# ==========================================
# 6. 主程序启动
# ==========================================
if __name__ == "__main__":
    print("\n>>> 开始执行策略流...")
    try:
        us_results = screen_us_stocks()
        write_to_sheet("Screener", us_results, sort_col="90D_Return%")
    except Exception as e:
        print(f"❌ 美股模块发生异常中断: {e}")
        
    try:
        a_results, a_diag_msg = screen_a_shares()
        write_to_sheet("A-Share Screener", a_results, sort_col="60D_Return%", diag_msg=a_diag_msg)
    except Exception as e:
        print(f"❌ A股模块发生异常中断: {e}")
