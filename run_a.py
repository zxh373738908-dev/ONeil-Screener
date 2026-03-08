import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, requests, json, re, time, concurrent.futures, warnings, traceback
from collections import defaultdict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
warnings.filterwarnings('ignore')

SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"  
creds = Credentials.from_service_account_file("credentials.json", scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
client = gspread.authorize(creds)

def get_sina_market_snapshot():
    all_data =[]
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'http://finance.sina.com.cn/'}
    for page in range(1, 80):
        try:
            res = requests.get(f"http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=80&sort=symbol&asc=1&node=hs_a", headers=headers, timeout=5)
            text = res.text
            if text == "[]" or text == "null" or not text: break
            text = re.sub(r'([{,])\s*([a-zA-Z0-9_]+)\s*:', r'\1"\2":', text)
            all_data.extend(json.loads(text))
        except: continue
    return pd.DataFrame(all_data)

def safe_float(val, default=0.0):
    try: return float(val)
    except: return default

def process_single_stock(row, session):
    pure_code = str(row['code'])[-6:] 
    name = row['name']
    if pure_code.startswith(('6', '5')): prefix = "1"
    elif pure_code.startswith(('0', '3')): prefix = "0"
    else: return {"status": "ignore"} 
    
    time.sleep(np.random.uniform(0.1, 0.4))
    try:
        url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={prefix}.{pure_code}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=1&end=20500000&lmt=300"
        res = session.get(url, timeout=10)
        if res.status_code != 200: return {"status": "fail", "reason": f"拦截({res.status_code})"}
        
        data_json = res.json()
        if not data_json or 'data' not in data_json or not data_json['data'] or 'klines' not in data_json['data']: 
            return {"status": "fail", "reason": "无数据"}
            
        klines = data_json['data']['klines']
        if len(klines) < 250: return {"status": "fail", "reason": "次新/退市"}
        
        closes, highs, lows, vols = [], [], [],[]
        for k in klines:
            p = k.split(',')
            closes.append(safe_float(p[2]))
            highs.append(safe_float(p[3]))
            lows.append(safe_float(p[4]))
            vols.append(safe_float(p[5]))
        
        cs, hs, ls, vs = pd.Series(closes), pd.Series(highs), pd.Series(lows), pd.Series(vols)
        close, close_60 = cs.iloc[-1], cs.iloc[-61]
        if close == 0.0: return {"status": "fail", "reason": "停牌"}
        
        ma20, ma50, ma150, ma200 = cs.rolling(20).mean().iloc[-1], cs.rolling(50).mean().iloc[-1], cs.rolling(150).mean().iloc[-1], cs.rolling(200).mean().iloc[-1]
        if not (close > ma50 and ma50 > ma150 and ma150 > ma200): return {"status": "fail", "reason": "非多头排列"}
            
        h250, l250 = hs.rolling(250).max().iloc[-1], ls.rolling(250).min().iloc[-1]
        if close < (h250 * 0.75): return {"status": "fail", "reason": "回撤>25%"}
        if close < (l250 * 1.25): return {"status": "fail", "reason": "底部反弹<25%"}
        
        ret_60 = (close - close_60) / close_60 if close_60 > 0 else 0
        if ret_60 < 0.15: return {"status": "fail", "reason": "动量<15%"}
            
        delta = cs.diff()
        up, down = delta.clip(lower=0), -1 * delta.clip(upper=0)
        rs = up.ewm(com=13, adjust=False).mean() / down.ewm(com=13, adjust=False).mean()
        rsi = 100 - (100 / (1 + rs)).iloc[-1]
        if rsi < 50: return {"status": "fail", "reason": "RSI弱势"}
        
        avg_v50 = vs.tail(50).mean()
        vol_ratio = vs.iloc[-1] / avg_v50 if avg_v50 > 0 else 0
        
        data = {
            "Ticker": pure_code, "Name": name, "Price": round(close, 2), "60D_Return%": f"{round(ret_60 * 100, 2)}%",
            "RSI": round(rsi, 2), "Turnover_Rate%": f"{row['turnoverratio']}%", "Vol_Ratio": round(vol_ratio, 2),
            "Dist_High%": f"{round(((close - h250) / h250) * 100, 2) if h250>0 else 0}%",
            "Mkt_Cap(亿)": round(row['mktcap'] / 100000000, 2), "Turnover(亿)": round(row['amount'] / 100000000, 2),
            "Trend": "Hold MA50"
        }
        return {"status": "success", "data": data, "log": f"✅ 捕获主升浪: {pure_code} {name}"}
    except requests.exceptions.Timeout: return {"status": "fail", "reason": "访问超时"}
    except requests.exceptions.ConnectionError: return {"status": "fail", "reason": "网络阻断"}
    except Exception as e: return {"status": "fail", "reason": f"异常:{str(e)[:10]}"}

def screen_a_shares():
    print("\n========== 开始处理 A股 ==========")
    spot_df = get_sina_market_snapshot()
    if spot_df.empty: return [], "❌ 大盘数据为空"
    
    total = len(spot_df)
    for col in['trade','mktcap','amount','turnoverratio']: spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce')
    spot_df['mktcap'] *= 10000
    f_df = spot_df[(spot_df['trade']>=10) & (spot_df['mktcap']>=5000000000) & (spot_df['amount']>=200000000) & (spot_df['turnoverratio']>=1.5)].copy()
    
    final_stocks =[]
    fail_reasons = defaultdict(int)
    
    session = requests.Session()
    retries = Retry(total=5, connect=5, read=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retries)
    session.mount('http://', adapter); session.mount('https://', adapter)
    session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': 'https://quote.eastmoney.com/'})

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_single_stock, row, session): row['code'] for _, row in f_df.iterrows()}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res["status"] == "success": final_stocks.append(res["data"])
            elif res["status"] == "fail": fail_reasons[res["reason"]] += 1

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fail_str = "".join([f"   - {r}: {c} 只\n" for r, c in sorted(fail_reasons.items(), key=lambda x:x[1], reverse=True)])
    return final_stocks, f"[{now}] 诊断报告：\n全市场: {total}只\n流动性达标: {len(f_df)}只\n明细：\n{fail_str}"

def write_to_sheet(sheet_name, final_stocks, sort_col, diag_msg=None):
    try:
        sheet = client.open_by_url(SHEET_URL).worksheet(sheet_name)
        sheet.clear()
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            df['Sort_Num'] = df[sort_col].str.replace('%', '').astype(float)
            df = df.sort_values(by='Sort_Num', ascending=False).drop(columns=['Sort_Num'])
            sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
            
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("M1", "Last Updated:")
            sheet.update_acell("N1", now)
            if diag_msg: sheet.update_acell("O1", diag_msg)
        else:
            sheet.update_acell("A1", diag_msg if diag_msg else "无符合条件的股票。")
    except Exception as e: print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    try:
        res, msg = screen_a_shares()
        write_to_sheet("A-Share Screener", res, "60D_Return%", diag_msg=msg)
    except Exception as e:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_info = traceback.format_exc()
        write_to_sheet("A-Share Screener", [], "60D_Return%", diag_msg=f"[{now}] 致命崩溃:\n{error_info}")
