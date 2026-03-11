import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, requests, json, re, concurrent.futures, warnings, traceback, random, time
from collections import defaultdict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import io

warnings.filterwarnings('ignore')

# ==========================================
# 1. 基础设置与双向 Google Sheets 连接
# ==========================================
SECTOR_SHEET_URL = "https://docs.google.com/spreadsheets/d/1hqqKmkJ6i5qCyqKK6l0__us1WmisK6cNTkDqh8yZSac/edit?gid=0#gid=0"
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

def get_robust_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://quote.eastmoney.com/'
    })
    return session

# ==========================================
# 2. 板块宏观模型
# ==========================================
def get_core_tickers_from_sheet(session):
    print("\n🌍[STEP 1] 尝试连接宏观大盘，寻找热点板块...")
    try:
        csv_url = SECTOR_SHEET_URL.replace("/edit?", "/export?format=csv&").replace("#gid=", "&gid=")
        try:
            res = session.get(csv_url, timeout=10)
            res.raise_for_status()
            raw_df = pd.read_csv(io.StringIO(res.text), header=None)
        except:
            doc = client.open_by_url(SECTOR_SHEET_URL)
            raw_data = doc.worksheets()[0].get_all_values()
            raw_df = pd.DataFrame(raw_data)

        header_idx = -1
        for i, row in raw_df.iterrows():
            if 'R120' in "".join([str(x).upper() for x in row.values]):
                header_idx = i; break
        if header_idx == -1: raise Exception("未找到表头")
            
        headers =[str(h).strip() if str(h).strip() else f"Unnamed_{i}" for i, h in enumerate(raw_df.iloc[header_idx].values)]
        df = pd.DataFrame(raw_df.iloc[header_idx + 1:].values, columns=headers)
        
        name_col = next((c for c in df.columns if '名' in c or 'Name' in str(c)), None)
        r120_col = next((c for c in df.columns if 'R120' in c.upper()), None)
        r60_col = next((c for c in df.columns if 'R60' in c.upper()), None)
        
        r120 = df[r120_col].apply(lambda x: parse_val(x, True))
        r60 = df[r60_col].apply(lambda x: parse_val(x, True))
        target_etfs = df[(r120 > 20.0) & (r60 > 0)][name_col].tolist()
        
        boards_map = {}
        for url in[
            "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2&fields=f12,f14",
            "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:3&fields=f12,f14"
        ]:
            try:
                res = session.get(url, timeout=5).json()
                for item in res['data']['diff']: boards_map[item['f14']] = item['f12']
            except: pass
                
        target_tickers = set()
        synonyms = {"化工":["化工行业", "磷化工", "煤化工", "基础化工", "化肥行业"]} # 添加化工防丢
        hardcoded_bk = {"化工":["BK0456", "BK0438"]}
        
        for etf_name in target_etfs:
            clean_name = re.sub(r'(ETF|LOF|指数|基金|增强|发起式|联接|A|C|类).*$', '', str(etf_name), flags=re.IGNORECASE).strip()
            if not clean_name: continue
            
            search_terms = set([clean_name])
            for key, aliases in synonyms.items():
                if key in clean_name or any(clean_name in a for a in aliases):
                    search_terms.update([key] + aliases)
            
            matched_b_codes = set()
            for term in search_terms:
                for b_name, b_code in boards_map.items():
                    if term in b_name or b_name in term: matched_b_codes.add(b_code)
            for key, bks in hardcoded_bk.items():
                if key in clean_name: matched_b_codes.update(bks)
            
            for b_code in matched_b_codes:
                try:
                    list_url = f"https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=b:{b_code}&fields=f12"
                    cons = session.get(list_url, timeout=5).json()
                    target_tickers.update([str(i['f12']).zfill(6) for i in cons['data']['diff']])
                except: pass
        return list(target_tickers)
    except: return[]

# ==========================================
# 3. 大盘扫描器
# ==========================================
def get_eastmoney_market_snapshot(session):
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1", "pz": "6000", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": "2", "invt": "2",
        "fid": "f3", "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
        "fields": "f12,f14,f2,f18,f20"
    }
    for _ in range(3):
        try:
            res = session.get(url, params=params, timeout=10).json()
            df = pd.DataFrame(res['data']['diff'])
            df.rename(columns={'f12': 'code', 'f14': 'name', 'f2': 'trade', 'f18': 'prev_close', 'f20': 'mktcap'}, inplace=True)
            return df
        except: time.sleep(1)
    return pd.DataFrame()

# ==========================================
# 4. K线运算与底层加速 (新增“缩量伏击”逻辑)
# ==========================================
def fetch_kline_data(secid, session):
    url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f61&klt=101&fqt=1&end=20500000&lmt=300"
    for _ in range(3):
        try:
            res = session.get(url, timeout=4).json()
            return res['data']['klines']
        except: time.sleep(0.1)
    return None

def process_single_stock(row, session):
    pure_code = str(row['code'])[-6:] 
    name = row['name']
    
    if pure_code.startswith(('6', '5')): prefix = "1"
    elif pure_code.startswith(('0', '3', '8', '4', '9')): prefix = "0"
    else: return {"status": "fail", "reason": "非A股"}
        
    try:
        klines = fetch_kline_data(f"{prefix}.{pure_code}", session)
        if not klines: return {"status": "fail", "reason": "无K线"}
        
        valid_klines =[k.split(',') for k in klines if len(k.split(',')) >= 8]
        if len(valid_klines) < 250: return {"status": "fail", "reason": "次新/退市"}

        k_matrix = np.array(valid_klines)
        closes = k_matrix[:, 2].astype(float)
        highs = k_matrix[:, 3].astype(float)
        lows = k_matrix[:, 4].astype(float)
        vols = k_matrix[:, 5].astype(float)
        amounts = k_matrix[:, 6].astype(float) 
        turnovers = k_matrix[:, 7].astype(float) 

        last_amount = amounts[-1]
        last_turnover = turnovers[-1]
        close = closes[-1]
        
        # ⚠️ 修改点1：放宽大盘股的换手率要求。000830 这种大票回调时换手率可能极低。
        # 只要成交额>1.5亿，即使换手率不到1%也允许通过。
        if last_amount < 150000000: return {"status": "fail", "reason": "成交额<1.5亿"}
        if last_turnover < 0.6 and last_amount < 300000000: return {"status": "fail", "reason": "流动性不足"}

        ma20 = np.mean(closes[-20:])
        ma50 = np.mean(closes[-50:])
        ma150 = np.mean(closes[-150:])
        ma200 = np.mean(closes[-200:])
        
        # 中长线趋势必须向好 (过滤下跌趋势)
        if not (ma50 > ma150 and ma150 > ma200): return {"status": "fail", "reason": "长线趋势未走好"}
        if ma200 < np.mean(closes[-220:-20]): return {"status": "fail", "reason": "年线未向上"}

        h250 = np.max(highs[-250:])
        if close < (h250 * 0.82): return {"status": "fail", "reason": "距新高太远"}
        if close > (ma50 * 1.25): return {"status": "fail", "reason": "偏离50日线极度超买"}

        avg_v50 = np.mean(vols[-50:])
        pct_change_3d = (close - closes[-4]) / closes[-4] if closes[-4] > 0 else 0
        vol_ratio_today = vols[-1] / avg_v50 if avg_v50 > 0 else 0

        deltas = np.diff(closes[-30:])
        up = pd.Series(np.where(deltas > 0, deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
        down = pd.Series(np.where(deltas < 0, -deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
        rsi = 100 - (100 / (1 + (up/down))) if down > 0 else 100

        # ==========================================
        # 🌟 核心修改点：双引擎判定 (突破 或 伏击)
        # ==========================================
        
        # 引擎A：右侧突破 (原逻辑，抓今天正在狂飙的)
        is_breakout = (
            close > ma20 and close > ma50 and 
            pct_change_3d >= 0.045 and 
            np.max(vols[-3:]) / avg_v50 >= 1.5 and 
            rsi >= 60
        )
        
        # 引擎B：左侧缩量伏击 (抓 000830 昨天那种情况)
        # 1. 价格贴近20日或50日均线 (上下 4% 以内)
        near_ma = (abs(close - ma20)/ma20 < 0.04) or (abs(close - ma50)/ma50 < 0.04)
        # 2. 缩量洗盘：今天的量不能太大，最好低于 50日均量 (或者没爆量)
        shrinking_vol = vol_ratio_today < 1.1
        # 3. 近期没有暴跌破位，而是温和震荡 (-5% 到 +4% 之间)
        mild_consolidation = -0.05 < pct_change_3d < 0.04
        # 4. 必须守住 50日生命线
        hold_trend = close >= ma50 * 0.98

        is_ambush = near_ma and shrinking_vol and mild_consolidation and hold_trend
        
        # 两个都不符合，则淘汰
        if not (is_breakout or is_ambush):
            return {"status": "fail", "reason": "未达突破标准，亦非标准缩量回踩"}

        # 打上标签，告诉你这是属于哪种机会
        trend_status = "🔥 右侧突破起飞" if is_breakout else "🧘‍♂️ 左侧缩量伏击(踩均线)"

        ret_60 = (close - closes[-61]) / closes[-61] if closes[-61] > 0 else 0
        data = {
            "Ticker": pure_code, 
            "Name": name, 
            "Price": round(close, 2), 
            "Type": trend_status,  # 新增列：买点类型
            "60D_Return%": f"{round(ret_60 * 100, 2)}%",
            "RSI": round(rsi, 2), 
            "Turnover_Rate%": f"{last_turnover}%", 
            "Vol_Ratio": round(vol_ratio_today, 2),
            "Dist_High%": f"{round(((close - h250) / h250) * 100, 2)}%",
            "Mkt_Cap(亿)": round(row['mktcap'] / 100000000, 2), 
            "Turnover(亿)": round(last_amount / 100000000, 2)
        }
        return {"status": "success", "data": data}
        
    except Exception as e: 
        return {"status": "fail", "reason": "解析异常"}

# ==========================================
# 5. 主程序控制
# ==========================================
def screen_a_shares():
    print("\n========== A股 猎手双引擎版 (包含右侧突破+左侧伏击) ==========")
    session = get_robust_session()
    core_tickers = get_core_tickers_from_sheet(session)
    spot_df = get_eastmoney_market_snapshot(session)
    if spot_df.empty: return[], "❌ 大盘数据为空"
    
    total = len(spot_df)
    for col in['trade', 'prev_close', 'mktcap']: 
        spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce')
    
    spot_df['trade'] = spot_df['trade'].fillna(spot_df['prev_close'])
    spot_df.loc[spot_df['trade'] == 0, 'trade'] = spot_df['prev_close']
    spot_df['pure_code'] = spot_df['code'].apply(lambda x: str(x)[-6:])
    
    if core_tickers:
        print(f"🎯 启用主线模式：将仅扫描板块提取出的 {len(core_tickers)} 只标的。")
        f_df = spot_df[spot_df['pure_code'].isin(core_tickers)].copy()
    else:
        f_df = spot_df.copy()
        
    f_df = f_df[(f_df['trade'] >= 5) & (f_df['mktcap'] >= 4000000000)].copy()
    print(f"💰 基础过滤完成：剩余 {len(f_df)} 只候选标的！启动并发引擎...")
    
    final_stocks =[]
    fail_reasons = defaultdict(int)

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        futures = {executor.submit(process_single_stock, row, session): row['code'] for _, row in f_df.iterrows()}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res["status"] == "success": final_stocks.append(res["data"])
            elif res["status"] == "fail": fail_reasons[res["reason"]] += 1

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fail_str = "".join([f"   - {r}: {c} 只\n" for r, c in sorted(fail_reasons.items(), key=lambda x:x[1], reverse=True)])
    diag_msg = (
        f"[{now}] 诊断报告：\n"
        f"📊 全市场基数: {total}只 | 基础过滤池: {len(f_df)}只\n"
        f"🏆 最终选出真龙头: {min(len(final_stocks), 50)}只\n"
        f"🔪 深度过滤淘汰明细：\n{fail_str}"
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
            df = df.head(50) 
            # 重新排列列顺序，让 Type 标签靠前显眼
            cols =['Ticker', 'Name', 'Price', 'Type', '60D_Return%', 'RSI', 'Turnover_Rate%', 'Vol_Ratio', 'Dist_High%', 'Mkt_Cap(亿)', 'Turnover(亿)']
            df = df[cols]
            sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
            
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("M1", "Last Updated:")
            sheet.update_acell("N1", now)
            if diag_msg: sheet.update_acell("O1", diag_msg)
            print(f"🎉 成功将前 {len(df)} 只最强龙头写入表格！")
        else:
            sheet.update_acell("A1", diag_msg if diag_msg else "无符合条件的股票。")
    except Exception as e: print(f"❌ 写入失败: {e}")

if __name__ == "__main__":
    try:
        res, msg = screen_a_shares()
        write_to_sheet("A-Share Screener", res, "60D_Return%", diag_msg=msg)
    except Exception as e:
        error_info = traceback.format_exc()
        write_to_sheet("A-Share Screener",
