import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, requests, json, re, concurrent.futures, warnings, traceback
from collections import defaultdict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
warnings.filterwarnings('ignore')

# ==========================================
# 1. 基础设置与双向 Google Sheets 连接
# ==========================================
SECTOR_SHEET_URL = "https://docs.google.com/spreadsheets/d/1BoYIVL3lb8nZE3U1qAkuO3MTrM117x2qycN1RdrDZgo/edit?gid=0#gid=0"
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"

scopes =["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# 构造强健的 requests Session (加入重试机制防断连)
def get_robust_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
    return session

global_session = get_robust_session()

def parse_pct(val):
    if pd.isna(val) or str(val).strip() == '': return 0.0
    s = str(val).replace(',', '').strip()
    is_pct = '%' in s
    s = s.replace('%', '')
    try:
        f = float(s)
        if not is_pct and -2 <= f <= 2: return f * 100.0
        return f
    except: return 0.0

def parse_float(val):
    if pd.isna(val) or str(val).strip() == '': return 0.0
    try: return float(str(val).replace(',', '').strip())
    except: return 0.0

# ==========================================
# 2. [第一阶] 读取板块大方向 
# ==========================================
def get_target_sectors():
    print("\n🌍 [STEP 1] 正在连接宏观大盘，寻找板块景气度模型...")
    try:
        doc = client.open_by_url(SECTOR_SHEET_URL)
        raw_data =[]
        sheet_name, header_row_index = "", -1
        
        for ws in doc.worksheets():
            data = ws.get_all_values()
            if not data: continue
            for i, row in enumerate(data[:10]):
                row_str = "".join([str(h).upper() for h in row])
                if 'R120' in row_str or 'RANK' in row_str or 'NAME' in row_str:
                    raw_data, sheet_name, header_row_index = data, ws.title, i
                    break
            if header_row_index != -1: break
                
        if header_row_index == -1: return []
            
        headers =[str(h).strip() for h in raw_data[header_row_index]]
        df = pd.DataFrame(raw_data[header_row_index + 1:], columns=headers)
        
        def get_fuzzy_col(keywords, is_pct=True):
            for kw in keywords:
                for col in df.columns:
                    if kw.lower() in str(col).lower().replace(' ', ''):
                        return df[col].apply(parse_pct if is_pct else parse_float)
            return pd.Series(0.0, index=df.index)

        r120 = get_fuzzy_col(['R120', '120日'], True)
        rank = get_fuzzy_col(['Rank', '排名', '强度'], False)
        rel20, rel60, r60, r20, rel5 = get_fuzzy_col(['REL20']), get_fuzzy_col(['REL60']), get_fuzzy_col(['R60']), get_fuzzy_col(['R20']), get_fuzzy_col(['REL5'])
        
        name_col = df.columns[0]
        for col in df.columns:
            if '名' in col or 'Name' in str(col): 
                name_col = col; break
        
        # 核心板块筛选逻辑
        cond_main = (r120 > 20.0) & (rank >= 80.0) & (rel20 > 0) & (rel60 > 0) & (r60 > 0)
        cond_dip = (r120 > 15.0) & (r20 < 0) & (rel5 > 0)
        
        all_target_sectors = list(set(df[cond_main][name_col].tolist() + df[cond_dip][name_col].tolist()))
        print(f"✅ 锁定 {len(all_target_sectors)} 个主线/黄金坑板块: {', '.join(all_target_sectors[:5])} ...")
        return all_target_sectors
    except Exception as e:
        print(f"⚠️ 读取板块发生解析错误: {e}")
        return[]

# ==========================================
# 3. [第二阶] 锁定板块成分股
# ==========================================
def get_stocks_from_sectors(sector_names):
    if not sector_names: return set()
    print("\n🧬 [STEP 2] 启动原生 API 引擎提取成分股...")
    target_tickers = set()
    ignore_keywords =['日经', '纳斯达克', '纳指', '标普', '恒生', '港股', '国债', '债券', '中概']
    boards_map = {} 
    
    try:
        url_ind = "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!50&fields=f12,f14"
        url_con = "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:3+f:!50&fields=f12,f14"
        
        for url in[url_ind, url_con]:
            res = global_session.get(url, timeout=5).json()
            if res and 'data' in res and res['data'] and 'diff' in res['data']:
                for item in res['data']['diff']: boards_map[item['f14']] = item['f12'] 
        
        for name in sector_names:
            name_str = str(name)
            if any(ig in name_str for ig in ignore_keywords): continue
                
            clean_name = re.sub(r'(ETF|LOF|指数|行业|概念|华安|国泰|华泰|柏瑞|广发|易方达|富国|南方|博时|汇添富|嘉实|建信|华夏|银华|天弘|工银|招商|鹏华|联接|泰康|平安|上证|深证|中证|\s*\(.*?\)\s*|\s*（.*?）\s*)', '', name_str).strip()
            if len(clean_name) < 2: clean_name = name_str[:2]
            
            matched_code = next((c for n, c in boards_map.items() if clean_name == n), None)
            if not matched_code:
                matched_code = next((c for n, c in boards_map.items() if clean_name in n or n in clean_name), None)
            
            if matched_code:
                cons_url = f"http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=b:{matched_code}&fields=f12"
                cons_res = global_session.get(cons_url, timeout=5).json()
                if cons_res and 'data' in cons_res and cons_res['data'] and 'diff' in cons_res['data']:
                    target_tickers.update([str(item['f12']).zfill(6) for item in cons_res['data']['diff']])
        
        print(f"✅ 成分股提取完毕！共锁定 {len(target_tickers)} 只核心标的。")
        return target_tickers
    except Exception as e: return set()

# ==========================================
# 4. [第三阶] 新浪基础数据 
# ==========================================
def get_sina_market_snapshot():
    all_data =[]
    for page in range(1, 80):
        try:
            res = global_session.get(f"http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={page}&num=80&sort=symbol&asc=1&node=hs_a", timeout=5)
            text = res.text
            if text == "[]" or text == "null" or not text: break
            text = re.sub(r'([{,])\s*([a-zA-Z0-9_]+)\s*:', r'\1"\2":', text)
            all_data.extend(json.loads(text))
        except: continue
    return pd.DataFrame(all_data)

# ==========================================
# 5.[第四阶] 腾讯WEB极速 K 线引擎 (纯Numpy加速 + 策略收紧)
# ==========================================
def process_single_stock(row, session):
    pure_code = str(row['code'])[-6:] 
    name = row['name']
    prefix = "sh" if pure_code.startswith(('6', '5')) else "sz"
    
    try:
        k_url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={prefix}{pure_code},day,,,300,qfq"
        res = session.get(k_url, timeout=4).json()
        
        if res.get('code') != 0: return {"status": "fail", "reason": "接口无数据"}
            
        data_node = res['data'][f'{prefix}{pure_code}']
        klines = data_node.get('qfqday', data_node.get('day',[]))
        
        if len(klines) < 250: return {"status": "fail", "reason": "次新/退市"}
        
        # 【加速核心】：使用 Numpy 代替 Pandas，提速 500%
        closes = np.array([float(k[2]) for k in klines])
        highs = np.array([float(k[3]) for k in klines])
        lows = np.array([float(k[4]) for k in klines])
        vols = np.array([float(k[5]) for k in klines])
        
        close, close_60 = closes[-1], closes[-61]
        if close == 0.0: return {"status": "fail", "reason": "停牌"}
        
        # --- 策略收紧 1: 更严格的多头排列 (加入20日线) ---
        ma20, ma50 = np.mean(closes[-20:]), np.mean(closes[-50:])
        ma120, ma200 = np.mean(closes[-120:]), np.mean(closes[-200:])
        if not (close > ma20 and ma20 > ma50 and ma50 > ma120 and ma120 > ma200): 
            return {"status": "fail", "reason": "未达强多头排列(MA20>50>120)"}
            
        # --- 策略收紧 2: 严控回撤 ---
        h250, l250 = np.max(highs[-250:]), np.min(lows[-250:])
        if close < (h250 * 0.80): return {"status": "fail", "reason": "高位回撤>20%"}
        if close < (l250 * 1.30): return {"status": "fail", "reason": "底部反弹<30%"}
        
        # --- 策略收紧 3: 提高动量要求 ---
        ret_60 = (close - close_60) / close_60 if close_60 > 0 else 0
        if ret_60 < 0.20: return {"status": "fail", "reason": "60日动量<20%"}
            
        # --- 策略收紧 4: 量能要求 (当日放量) ---
        avg_v50 = np.mean(vols[-50:])
        vol_ratio = vols[-1] / avg_v50 if avg_v50 > 0 else 0
        if vol_ratio < 1.0: return {"status": "fail", "reason": "近期缩量(量比<1)"}
        
        # 近似 EWMA RSI 加速计算
        deltas = np.diff(closes[-30:])
        up = pd.Series(np.where(deltas > 0, deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
        down = pd.Series(np.where(deltas < 0, -deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
        rsi = 100 - (100 / (1 + (up/down))) if down > 0 else 100
        
        if rsi < 60: return {"status": "fail", "reason": "RSI不够强势(<60)"}
        
        data = {
            "Ticker": pure_code, "Name": name, "Price": round(close, 2), "60D_Return%": f"{round(ret_60 * 100, 2)}%",
            "RSI": round(rsi, 2), "Turnover_Rate%": f"{row['turnoverratio']}%", "Vol_Ratio": round(vol_ratio, 2),
            "Dist_High%": f"{round(((close - h250) / h250) * 100, 2) if h250>0 else 0}%",
            "Mkt_Cap(亿)": round(row['mktcap'] / 100000000, 2), "Turnover(亿)": round(row['amount'] / 100000000, 2),
            "Trend": "Super Bull"
        }
        return {"status": "success", "data": data}
    except Exception as e: 
        return {"status": "fail", "reason": "接口解析异常"}

# ==========================================
# 6. 主程序流转控制
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股 (极速龙头狙击版) ==========")
    
    target_sectors = get_target_sectors()
    if not target_sectors: return[], "宏观模型无热点，空仓保护！"
        
    core_tickers = get_stocks_from_sectors(target_sectors)
    if not core_tickers: return[], "成分股提取失败。"
    
    print("\n📊 [STEP 3] 扫描全市场流动性...")
    spot_df = get_sina_market_snapshot()
    if spot_df.empty: return[], "大盘数据为空"
    
    for col in ['trade','mktcap','amount','turnoverratio']: spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce')
    spot_df['mktcap'] *= 10000
    spot_df['pure_code'] = spot_df['code'].apply(lambda x: str(x)[-6:])
    
    f_df = spot_df[spot_df['pure_code'].isin(core_tickers)].copy()
    f_df = f_df[(f_df['trade']>=5) & (f_df['mktcap']>=5000000000) & (f_df['amount']>=200000000) & (f_df['turnoverratio']>=1.5)].copy()
    print(f"💰 流动性过滤完成：剩余 {len(f_df)} 只主力高换手标的！启动 K 线多线程引擎。")
    
    final_stocks =[]
    fail_reasons = defaultdict(int)

    # 提高并发数至15，配合连接池更快完成
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(process_single_stock, row, global_session): row['code'] for _, row in f_df.iterrows()}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res["status"] == "success": final_stocks.append(res["data"])
            elif res["status"] == "fail": fail_reasons[res["reason"]] += 1

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fail_str = "".join([f"   - {r}: {c} 只\n" for r, c in sorted(fail_reasons.items(), key=lambda x:x[1], reverse=True)])
    diag_msg = (
        f"[{now}] 诊断报告：\n"
        f"🔥 锁定主线板块: {len(target_sectors)} 个\n"
        f"🎯 流动性达标核心股: {len(f_df)} 只\n"
        f"🏆 最终选出真龙头: min({len(final_stocks)}, 50) 只\n"
        f"🔪 K线严格淘汰明细：\n{fail_str}"
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
            
            # 【核心截断】：如果选出太多，强制只保留前 50 只最暴力的龙头！
            df = df.head(50) 
            
            sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
            
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("M1", "Last Updated:")
            sheet.update_acell("N1", now)
            if diag_msg: sheet.update_acell("O1", diag_msg)
            print(f"🎉 成功将 {len(df)} 只最强龙头写入表格！")
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
