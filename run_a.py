import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, requests, json, re, concurrent.futures, warnings, traceback, random
from collections import defaultdict
warnings.filterwarnings('ignore')

# ==========================================
# 1. 基础设置与双向 Google Sheets 连接
# ==========================================
SECTOR_SHEET_URL = "https://docs.google.com/spreadsheets/d/1BoYIVL3lb8nZE3U1qAkuO3MTrM117x2qycN1RdrDZgo/edit?gid=0#gid=0"
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"

scopes =["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

def parse_val(val, is_pct=False):
    if pd.isna(val) or str(val).strip() == '': return 0.0
    s = str(val).replace(',', '').replace('%', '').strip()
    try:
        f = float(s)
        if not is_pct and -2 <= f <= 2: return f * 100.0
        return f
    except: return 0.0

# ==========================================
# 2. 板块宏观模型 (带有容错机制)
# ==========================================
def get_core_tickers_from_sheet():
    print("\n🌍 [STEP 1] 尝试连接宏观大盘，寻找热点板块...")
    try:
        doc = client.open_by_url(SECTOR_SHEET_URL)
        raw_data, header_idx =[], -1
        
        for ws in doc.worksheets():
            data = ws.get_all_values()
            if not data: continue
            for i, row in enumerate(data[:10]):
                if 'R120' in "".join([str(h).upper() for h in row]):
                    raw_data, header_idx = data, i; break
            if header_idx != -1: break
                
        if header_idx == -1: raise Exception("未找到表头")
            
        headers = [str(h).strip() for h in raw_data[header_idx]]
        df = pd.DataFrame(raw_data[header_idx + 1:], columns=headers)
        
        name_col = next((c for c in df.columns if '名' in c or 'Name' in str(c)), df.columns[0])
        r120 = df[next((c for c in df.columns if 'R120' in c.upper()), df.columns[0])].apply(lambda x: parse_val(x, True))
        r60 = df[next((c for c in df.columns if 'R60' in c.upper()), df.columns[0])].apply(lambda x: parse_val(x, True))
        
        # 提取极强板块
        target_sectors = df[(r120 > 20.0) & (r60 > 0)][name_col].tolist()
        if not target_sectors: raise Exception("无符合条件的热门板块")
            
        print(f"✅ 锁定 {len(target_sectors)} 个热点板块，准备提取成分股...")
        
        # 提取成分股（直接走东财API）
        target_tickers, boards_map = set(), {}
        for url in[
            "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!50&fields=f12,f14",
            "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:3+f:!50&fields=f12,f14"
        ]:
            res = requests.get(url, timeout=5).json()
            for item in res['data']['diff']: boards_map[item['f14']] = item['f12']
                
        for name in target_sectors:
            clean = re.sub(r'(ETF|LOF|指数|行业|概念|\s*\(.*?\)\s*)', '', str(name)).strip()
            b_code = next((c for n, c in boards_map.items() if clean in n), None)
            if b_code:
                cons = requests.get(f"http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=b:{b_code}&fields=f12", timeout=5).json()
                if cons and 'data' in cons and cons['data'] and 'diff' in cons['data']:
                    target_tickers.update([str(i['f12']).zfill(6) for i in cons['data']['diff']])
        return list(target_tickers)
    except Exception as e:
        print(f"⚠️ 板块宏观筛选未能生效 ({e})。系统将自动降级为【全市场扫描】！")
        return[]

# ==========================================
# 3. 新浪大盘扫描器 
# ==========================================
def get_sina_market_snapshot():
    print("🚀 启动【新浪财经】高匿雷达抓取基础数据...")
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

# ==========================================
# 4. [黑科技] 随机CDN节点 + Numpy极限加速运算
# ==========================================
def fetch_kline_data(secid, session):
    for _ in range(3):
        node = random.randint(1, 99)
        url = f"http://{node}.push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=1&end=20500000&lmt=300"
        try:
            res = session.get(url, timeout=3)
            if res.status_code == 200:
                data = res.json()
                if data and 'data' in data and data['data'] and 'klines' in data['data']:
                    return data['data']['klines']
        except: continue
    return None

def process_single_stock(row, session):
    pure_code = str(row['code'])[-6:] 
    name = row['name']
    
    if pure_code.startswith(('6', '5')): prefix = "1"
    elif pure_code.startswith(('0', '3')): prefix = "0"
    else: return {"status": "fail", "reason": "非A股标的"}
        
    try:
        klines = fetch_kline_data(f"{prefix}.{pure_code}", session)
        if not klines: return {"status": "fail", "reason": "节点阻断"}
        if len(klines) < 250: return {"status": "fail", "reason": "次新/退市"}

        # 【核心融合】：将你的字符串直接解析为 Numpy 数组，速度飙升 10 倍！
        k_matrix = np.array([k.split(',') for k in klines])
        closes = k_matrix[:, 2].astype(float)
        highs = k_matrix[:, 3].astype(float)
        lows = k_matrix[:, 4].astype(float)
        vols = k_matrix[:, 5].astype(float)
        
        close, close_60 = closes[-1], closes[-61]
        if close == 0.0: return {"status": "fail", "reason": "停牌"}
        
        # 使用Numpy计算均线
        ma50 = np.mean(closes[-50:])
        ma150 = np.mean(closes[-150:])
        ma200 = np.mean(closes[-200:])
        if not (close > ma50 and ma50 > ma150 and ma150 > ma200): 
            return {"status": "fail", "reason": "非多头排列"}
            
        h250, l250 = np.max(highs[-250:]), np.min(lows[-250:])
        if close < (h250 * 0.75): return {"status": "fail", "reason": "回撤>25%"}
        if close < (l250 * 1.25): return {"status": "fail", "reason": "底部反弹<25%"}
        
        ret_60 = (close - close_60) / close_60 if close_60 > 0 else 0
        if ret_60 < 0.15: return {"status": "fail", "reason": "动量<15%"}
        
        # RSI 矩阵近似运算
        deltas = np.diff(closes[-30:])
        up = pd.Series(np.where(deltas > 0, deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
        down = pd.Series(np.where(deltas < 0, -deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
        rsi = 100 - (100 / (1 + (up/down))) if down > 0 else 100
        
        if rsi < 50: return {"status": "fail", "reason": "RSI弱势"}
        
        avg_v50 = np.mean(vols[-50:])
        vol_ratio = vols[-1] / avg_v50 if avg_v50 > 0 else 0
        
        data = {
            "Ticker": pure_code, "Name": name, "Price": round(close, 2), "60D_Return%": f"{round(ret_60 * 100, 2)}%",
            "RSI": round(rsi, 2), "Turnover_Rate%": f"{row['turnoverratio']}%", "Vol_Ratio": round(vol_ratio, 2),
            "Dist_High%": f"{round(((close - h250) / h250) * 100, 2) if h250>0 else 0}%",
            "Mkt_Cap(亿)": round(row['mktcap'] / 100000000, 2), "Turnover(亿)": round(row['amount'] / 100000000, 2),
            "Trend": "Hold MA50"
        }
        return {"status": "success", "data": data}
    except Exception as e: return {"status": "fail", "reason": "解析异常"}

# ==========================================
# 5. 主程序控制 (智能降级机制)
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股 (CDN散列 + Numpy加速融合版) ==========")
    
    # 获取板块限制名单
    core_tickers = get_core_tickers_from_sheet()
    
    spot_df = get_sina_market_snapshot()
    if spot_df.empty: return[], "❌ 大盘数据为空"
    
    total = len(spot_df)
    for col in['trade','mktcap','amount','turnoverratio']: spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce')
    spot_df['mktcap'] *= 10000
    spot_df['pure_code'] = spot_df['code'].apply(lambda x: str(x)[-6:])
    
    # 🌟【智能容错逻辑】：如果有板块核心股，就过滤；如果宏观没有选出板块，直接扫描全市场！
    if core_tickers:
        print(f"🎯 启用主线模式：将仅扫描板块提取出的 {len(core_tickers)} 只标的。")
        f_df = spot_df[spot_df['pure_code'].isin(core_tickers)].copy()
    else:
        print(f"🌊 启用全市场扫描模式：对全部 {total} 只A股进行清洗！")
        f_df = spot_df.copy()
        
    # 流动性初筛
    f_df = f_df[(f_df['trade']>=10) & (f_df['mktcap']>=5000000000) & (f_df['amount']>=200000000) & (f_df['turnoverratio']>=1.5)].copy()
    
    print(f"💰 流动性过滤完成：剩余 {len(f_df)} 只高换手标的！启动东财CDN散射多线程。")
    
    final_stocks =[]
    fail_reasons = defaultdict(int)

    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': 'http://quote.eastmoney.com/'})

    # CDN 加持下，放心把线程开到 15！
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(process_single_stock, row, session): row['code'] for _, row in f_df.iterrows()}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res["status"] == "success": final_stocks.append(res["data"])
            elif res["status"] == "fail": fail_reasons[res["reason"]] += 1

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fail_str = "".join([f"   - {r}: {c} 只\n" for r, c in sorted(fail_reasons.items(), key=lambda x:x[1], reverse=True)])
    diag_msg = (
        f"[{now}] 诊断报告：\n"
        f"📊 全市场基数: {total}只 | 流动性达标: {len(f_df)}只\n"
        f"🏆 最终选出: {len(final_stocks)}只\n"
        f"🔪 K线淘汰明细：\n{fail_str}"
    )
    return final_stocks, diag_msg

def write_to_sheet(sheet_name, final_stocks, sort_col, diag_msg=None):
    try:
        sheet = client.open_by_url(OUTPUT_SHEET_URL).worksheet(sheet_name)
        sheet.clear()
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            df['Sort_Num'] = df[sort_col].str.replace('%', '').astype(float)
            df = df.sort_values(by='Sort_Num', ascending=False).drop(columns=['Sort_Num'])
            
            # 【截断机制】如果是全市场扫描，可能选出几百只，我们强制只保留涨幅最猛的前 50 只
            df = df.head(50) 
            
            sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
            
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("M1", "Last Updated:")
            sheet.update_acell("N1", now)
            if diag_msg: sheet.update_acell("O1", diag_msg)
            print(f"🎉 成功将前 {len(df)} 只最强龙头写入表格！")
            print(diag_msg)
        else:
            sheet.update_acell("A1", diag_msg if diag_msg else "无符合条件的股票。")
            print("⚠️ 筛选结束，已写入空仓诊断报告。")
            print(diag_msg)
    except Exception as e: print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    try:
        res, msg = screen_a_shares()
        write_to_sheet("A-Share Screener", res, "60D_Return%", diag_msg=msg)
    except Exception as e:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_info = traceback.format_exc()
        write_to_sheet("A-Share Screener", [], "60D_Return%", diag_msg=f"[{now}] 致命崩溃:\n{error_info}")
