import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import requests
import json
import re
import time
import concurrent.futures
from collections import defaultdict
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
# 2. 新浪财经大盘隐身雷达
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
        except: continue
    df = pd.DataFrame(all_data)
    print(f"✅ 获取 A 股 {len(df)} 只股票基础数据成功！")
    return df

def safe_float(val, default=0.0):
    try:
        return float(val)
    except:
        return default

# ==========================================
# 3. 东方财富极速 K线核心逻辑 (带欧奈尔趋势模板)
# ==========================================
def process_single_stock(row, session):
    pure_code = str(row['code'])[-6:] 
    name = row['name']
    
    if pure_code.startswith(('6', '5')): prefix = "1"
    elif pure_code.startswith(('0', '3')): prefix = "0"
    else: return {"status": "ignore"} 

    # 引入微弱随机停顿防屏蔽
    time.sleep(np.random.uniform(0.1, 0.3))

    try:
        secid = f"{prefix}.{pure_code}"
        # 【重要修复1】：把 end 改回 20500000，这是东财唯一的合法"最新"标识符
        url = f"http://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=1&end=20500000&lmt=300"
        
        res = session.get(url, timeout=8)
        
        if res.status_code != 200:
            return {"status": "fail", "reason": f"HTTP访问拦截({res.status_code})"}
            
        data_json = res.json()
        if not data_json or 'data' not in data_json or not data_json['data'] or 'klines' not in data_json['data']:
            return {"status": "fail", "reason": "东财无返回数据"}
            
        klines = data_json['data']['klines']
        if len(klines) < 250:
            return {"status": "fail", "reason": "次新股或退市停牌"}
        
        closes, highs, lows, vols = [], [], [],[]
        for k in klines:
            parts = k.split(',')
            closes.append(safe_float(parts[2]))
            highs.append(safe_float(parts[3]))
            lows.append(safe_float(parts[4]))
            vols.append(safe_float(parts[5]))
        
        close_series = pd.Series(closes)
        high_series = pd.Series(highs)
        low_series = pd.Series(lows)
        vol_series = pd.Series(vols)
        
        close = close_series.iloc[-1]
        close_60 = close_series.iloc[-61]
        
        if close == 0.0: return {"status": "fail", "reason": "停牌无有效价格"}
        
        ma20 = close_series.rolling(20).mean().iloc[-1]
        ma50 = close_series.rolling(50).mean().iloc[-1]
        ma150 = close_series.rolling(150).mean().iloc[-1]
        ma200 = close_series.rolling(200).mean().iloc[-1]
        
        if not (close > ma50 and ma50 > ma150 and ma150 > ma200): 
            return {"status": "fail", "reason": "均线非多头排列"}
            
        high_250 = high_series.rolling(250).max().iloc[-1]
        low_250 = low_series.rolling(250).min().iloc[-1]
        
        if close < (high_250 * 0.75): 
            return {"status": "fail", "reason": "距高点回撤超25%"}
        if close < (low_250 * 1.25):
            return {"status": "fail", "reason": "底部反弹不足25%"}
        
        ret_60 = (close - close_60) / close_60 if close_60 > 0 else 0
        if ret_60 < 0.15: 
            return {"status": "fail", "reason": "60日动量不足15%"}
            
        delta = close_series.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        rs = up.ewm(com=13, adjust=False).mean() / down.ewm(com=13, adjust=False).mean()
        rsi = 100 - (100 / (1 + rs)).iloc[-1]
        if rsi < 50: 
            return {"status": "fail", "reason": "RSI弱势"}
        
        avg_vol_50 = vol_series.tail(50).mean()
        vol_ratio = vol_series.iloc[-1] / avg_vol_50 if avg_vol_50 > 0 else 0
        dist_high = (close - high_250) / high_250 if high_250 > 0 else 0
        mkt_cap_yi = row['mktcap'] / 100_000_000
            
        data = {
            "Ticker": pure_code, 
            "Name": name, 
            "Price": round(close, 2),
            "60D_Return%": f"{round(ret_60 * 100, 2)}%",
            "RSI": round(rsi, 2),
            "Turnover_Rate%": f"{row['turnoverratio']}%",
            "Vol_Ratio": round(vol_ratio, 2),
            "Dist_High%": f"{round(dist_high * 100, 2)}%",
            "Mkt_Cap(亿)": round(mkt_cap_yi, 2),
            "Turnover(亿)": round(row['amount'] / 100_000_000, 2),
            "Trend": "Hold MA50"
        }
        return {"status": "success", "data": data, "log": f"✅ 捕获欧奈尔主升浪: {pure_code} {name}"}
        
    except requests.exceptions.Timeout:
        return {"status": "fail", "reason": "网络访问超时"}
    except requests.exceptions.ConnectionError:
        return {"status": "fail", "reason": "网络被强行切断"}
    except Exception as e:
        err_msg = str(e)[:15]
        return {"status": "fail", "reason": f"异常报错: {err_msg}"}

# ==========================================
# 4. 主干过滤与多线程执行
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股[终极修复版] ==========")
    try:
        spot_df = get_sina_market_snapshot()
        if spot_df.empty: return[], "❌ 接口获取失败，大盘数据为空。"
            
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
        print(f"🎯 流动性初筛: 满足核心标的剩余 {liquidity_passed} 只。开始启动安全并发引擎...")

        final_a_stocks =[]
        fail_reasons = defaultdict(int)
        
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retries)
        # 【重要修复2】：必须挂载 http 协议，否则高并发池直接失效阻塞
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Referer': 'http://quote.eastmoney.com/'
        })

        processed_count = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_code = {executor.submit(process_single_stock, row, session): str(row['code']) for index, row in filtered_df.iterrows()}
            
            for future in concurrent.futures.as_completed(future_to_code):
                processed_count += 1
                if processed_count % 100 == 0:
                    print(f"   ⚡ ...安全扫描中，已完成 {processed_count}/{liquidity_passed} 只A股...")
                    
                res = future.result()
                if res["status"] == "success":
                    final_a_stocks.append(res["data"])
                    print(res["log"])
                elif res["status"] == "fail":
                    fail_reasons[res["reason"]] += 1

        now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        fail_details_str = ""
        for reason, count in sorted(fail_reasons.items(), key=lambda item: item[1], reverse=True):
            fail_details_str += f"   - 【{reason}】: {count} 只\n"
            
        diag_msg = (
            f"[{now_time}] 欧奈尔系统诊断报告：\n"
            f"1. 全市场扫描：获取 {total_stocks} 只股票\n"
            f"2. 流动性初筛：剩余 {liquidity_passed} 只标的\n"
            f"3. 淘汰明细：\n"
            f"{fail_details_str}"
            f"结论：多线程系统已极速完成最新检测。"
        )
        return final_a_stocks, diag_msg
    except Exception as e:
        return[], f"代码发生内部错误: {e}"

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
            data_to_write =[df.columns.values.tolist()] + df.values.tolist()
            sheet.clear()
            sheet.update(values=data_to_write, range_name="A1")
            
            now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("I1", "Last Updated:")
            sheet.update_acell("J1", now_time)
            print(f"🎉 成功将 {len(df)} 只标的写入 {sheet_name}！")
        else:
            sheet.clear()
            final_msg = diag_msg if diag_msg else f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 当下无符合条件的股票。"
            sheet.update_acell("A1", final_msg)
            print(f"⚠️ {sheet_name}: 已输出诊断报告/空仓警告。")
    except Exception as e:
        print(f"❌ 写入 {sheet_name} 失败: {e}")

if __name__ == "__main__":
    a_results, a_diag_msg = screen_a_shares()
    write_to_sheet("A-Share Screener", a_results, sort_col="60D_Return%", diag_msg=a_diag_msg)
