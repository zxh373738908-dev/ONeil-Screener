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
    adapter = HTTPAdapter(max_retries=retry, pool_connections=30, pool_maxsize=30)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://quote.eastmoney.com/',
        'Accept': '*/*',
        'Connection': 'keep-alive'
    })
    return session

# ==========================================
# 2. 板块宏观模型 (A股专用)
# ==========================================
def get_core_tickers_from_sheet(session):
    print("\n🌍[STEP 1] 尝试连接宏观大盘，寻找A股热点板块...")
    try:
        csv_url = SECTOR_SHEET_URL.replace("/edit?", "/export?format=csv&").replace("#gid=", "&gid=")
        try:
            res = session.get(csv_url, timeout=10)
            res.raise_for_status()
            raw_df = pd.read_csv(io.StringIO(res.text), header=None)
        except Exception as csv_e:
            print(f"⚠️ CSV快读受阻 ({csv_e})，降级为 gspread API 读取...")
            doc = client.open_by_url(SECTOR_SHEET_URL)
            raw_data = doc.worksheets()[0].get_all_values()
            raw_df = pd.DataFrame(raw_data)

        header_idx = -1
        for i, row in raw_df.iterrows():
            row_str = "".join([str(x).upper() for x in row.values])
            if 'R120' in row_str:
                header_idx = i; break
                
        if header_idx == -1: raise Exception("未在表格中找到包含 R120 的表头行")
            
        headers = []
        for h in raw_df.iloc[header_idx].values:
            h_str = str(h).strip()
            if not h_str or h_str in headers:
                headers.append(f"Unnamed_{len(headers)}")
            else:
                headers.append(h_str)
                
        df = pd.DataFrame(raw_df.iloc[header_idx + 1:].values, columns=headers)
        
        name_col = next((c for c in df.columns if '名' in c or 'Name' in str(c)), None)
        r120_col = next((c for c in df.columns if 'R120' in c.upper()), None)
        r60_col = next((c for c in df.columns if 'R60' in c.upper()), None)
        
        if not name_col or not r120_col or not r60_col:
            raise Exception("缺失必要列(Name/R120/R60)，请检查表格")
            
        r120 = df[r120_col].apply(lambda x: parse_val(x, True))
        r60 = df[r60_col].apply(lambda x: parse_val(x, True))
        
        target_etfs = df[(r120 > 20.0) & (r60 > 0)][name_col].tolist()
        if not target_etfs: raise Exception("无符合 R120>20% 且 R60>0 的热门板块")
            
        print(f"✅ 锁定 {len(target_etfs)} 个热点ETF，准备智能映射A股成分股...")
        
        boards_map = {}
        for url in[
            "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2&fields=f12,f14", 
            "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:3&fields=f12,f14"  
        ]:
            for _ in range(3):
                try:
                    res = session.get(url, timeout=5).json()
                    if res and 'data' in res and res['data']:
                        for item in res['data']['diff']: boards_map[item['f14']] = item['f12']
                    break
                except: time.sleep(1)
                
        target_tickers = set()
        
        synonyms = {
            "航空航天":["航天航空", "大飞机", "卫星通信", "国防军工", "军工"],
            "通用航空":["通用航空", "低空经济", "飞行汽车", "大飞机", "航天航空"],
            "有色金属":["小金属", "工业金属", "能源金属", "稀缺资源", "基本金属"],
            "黄金":["贵金属", "黄金概念", "珠宝首饰"],
            "煤炭":["煤炭行业", "煤炭概念"],
            "钢铁": ["钢铁行业", "特钢概念"],
            "光伏": ["光伏设备", "光伏概念", "太阳能", "BC电池"],
            "新能源车": ["汽车整车", "汽车零部件", "新能源车概念"],
            "新能源":["风电设备", "光伏设备", "电池", "绿色电力"],
            "传媒":["文化传媒", "游戏", "短剧互动游戏"],
            "芯片": ["半导体", "芯片概念", "存储芯片"],
            "医药":["化学制药", "中药", "生物制品", "医药商业", "医疗器械", "创新药"],
            "军工":["航天航空", "船舶制造", "兵器装备", "军工概念"],
            "通信":["通信设备", "通信服务", "5G概念", "6G概念", "CPO概念"],
            "软件":["软件开发", "IT服务", "信创"],
            "信创":["信创产业", "国产软件", "数字经济", "数据安全", "软件开发"]
        }
        
        hardcoded_bk = {
            "黄金":["BK0477", "BK0717"], "煤炭":["BK0437", "BK0532"], 
            "有色金属":["BK0478", "BK0479", "BK0496"], "光伏":["BK1031", "BK0854"], 
            "航空航天":["BK0480", "BK0498"], "通用航空": ["BK0902", "BK1166"], 
            "钢铁":["BK0470", "BK0539"], "信创":["BK1104", "BK0737"]      
        }
        
        overseas_broad_keywords =['日经', '纳指', '标普', '恒生', '港股', '德国', '法国', '亚洲', '中概', '中证', '沪深', '上证', '深证', '科创', '创业板50', '双创', '红利低波']

        for etf_name in target_etfs:
            if any(k in str(etf_name).upper() for k in overseas_broad_keywords):
                continue

            clean_name = re.sub(r'(ETF|LOF|指数|基金|增强|发起式|联接|A|C|类).*$', '', str(etf_name), flags=re.IGNORECASE).strip()
            if not clean_name: continue
            
            search_terms = set([clean_name])
            for key, aliases in synonyms.items():
                if key in clean_name or clean_name in key or any(clean_name in a or a in clean_name for a in aliases):
                    search_terms.add(key)
                    search_terms.update(aliases)
            
            matched_b_codes = set()
            for term in search_terms:
                for b_name, b_code in boards_map.items():
                    if term in b_name or b_name in term: matched_b_codes.add(b_code)
                        
            for key, bks in hardcoded_bk.items():
                if key in clean_name or clean_name in key: matched_b_codes.update(bks)
            
            if matched_b_codes:
                for b_code in matched_b_codes:
                    for _ in range(3):
                        try:
                            list_url = f"https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=b:{b_code}&fields=f12"
                            cons = session.get(list_url, timeout=5).json()
                            if cons and 'data' in cons and cons['data'] and 'diff' in cons['data']:
                                target_tickers.update([str(i['f12']).zfill(6) for i in cons['data']['diff']])
                            break
                        except: time.sleep(1)
        if not target_tickers: raise Exception("未能匹配到A股成分股")
        return list(target_tickers)
        
    except Exception as e:
        print(f"⚠️ 板块宏观筛选未能生效 ({type(e).__name__}: {str(e)})。系统自动降级为全市场扫描！")
        return[]

# ==========================================
# 3. 市场大盘数据获取 (A股 + 港股)
# ==========================================
def get_eastmoney_market_snapshot(session):
    print("🚀 启动【东方财富】抓取 A股 全市场基础代码库...")
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1", "pz": "6000", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": "2", "invt": "2",
        "fid": "f3", "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
        "fields": "f12,f14,f2,f18,f20"
    }
    for _ in range(3):
        try:
            res = session.get(url, params=params, timeout=10).json()
            if res and 'data' in res and res['data'] and 'diff' in res['data']:
                df = pd.DataFrame(res['data']['diff'])
                df.rename(columns={'f12': 'code', 'f14': 'name', 'f2': 'trade', 'f18': 'prev_close', 'f20': 'mktcap'}, inplace=True)
                return df
        except Exception: time.sleep(1)
    return pd.DataFrame()

def get_hk_market_snapshot(session):
    print("🚀 启动【东方财富】抓取 港股(HK) 全市场基础代码库...")
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    # m:116 是港股主板的核心标志
    params = {
        "pn": "1", "pz": "4000", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": "2", "invt": "2",
        "fid": "f3", "fs": "m:116+t:3,m:116+t:4,m:116+t:1,m:116+t:2", 
        "fields": "f12,f14,f2,f18,f20"
    }
    for _ in range(3):
        try:
            res = session.get(url, params=params, timeout=10).json()
            if res and 'data' in res and res['data'] and 'diff' in res['data']:
                df = pd.DataFrame(res['data']['diff'])
                df.rename(columns={'f12': 'code', 'f14': 'name', 'f2': 'trade', 'f18': 'prev_close', 'f20': 'mktcap'}, inplace=True)
                return df
        except Exception: time.sleep(1)
    return pd.DataFrame()

# ==========================================
# 4. K线核心引擎 (复用逻辑)
# ==========================================
def fetch_kline_data(secid, session):
    url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f61&klt=101&fqt=1&end=20500000&lmt=300"
    for _ in range(3):
        try:
            res = session.get(url, timeout=4)
            if res.status_code == 200:
                data = res.json()
                if data and 'data' in data and data['data'] and 'klines' in data['data']:
                    return data['data']['klines']
        except Exception:
            time.sleep(random.uniform(0.1, 0.3))
    return None

def apply_oneil_logic(code, name, klines, mktcap, market_type="A"):
    # 抽取出的欧奈尔核心运算逻辑，供A股和港股共用
    valid_klines =[k.split(',') for k in klines if len(k.split(',')) >= 8]
    if len(valid_klines) < 250: return {"status": "fail", "reason": "次新/退市"}

    k_matrix = np.array(valid_klines)
    closes = k_matrix[:, 2].astype(float)
    highs = k_matrix[:, 3].astype(float)
    lows = k_matrix[:, 4].astype(float)
    vols = k_matrix[:, 5].astype(float)
    amounts = k_matrix[:, 6].astype(float) 
    turnovers = k_matrix[:, 7].astype(float) 
    
    if vols[-1] == 0: return {"status": "fail", "reason": "停牌"}

    last_amount = amounts[-1]
    last_turnover = turnovers[-1]
    close = closes[-1]
    if close == 0.0: return {"status": "fail", "reason": "停牌"}
    
    # 市场独立的流动性防爆墙
    if market_type == "A":
        if last_amount < 200000000: return {"status": "fail", "reason": "成交额<2亿"}
        if last_turnover < 1.5: return {"status": "fail", "reason": "换手率<1.5%"}
    else: # 港股 HK
        # 港股因为有腾讯这种巨无霸，换手率可能很低(0.2%)，所以不考核换手率，只卡绝对成交额（>5000万港币）
        if last_amount < 50000000: return {"status": "fail", "reason": "成交极度萎靡(<5000万)"}

    # === 欧奈尔/米尔维尼 核心趋势模板 ===
    ma20 = np.mean(closes[-20:])
    ma50 = np.mean(closes[-50:])
    ma150 = np.mean(closes[-150:])
    ma200 = np.mean(closes[-200:])
    ma200_20d_ago = np.mean(closes[-220:-20]) 
    
    if not (close > ma20 and close > ma50 and ma50 > ma150 and ma150 > ma200): 
        return {"status": "fail", "reason": "非标准多头排列"}
    if ma200 < ma200_20d_ago:
        return {"status": "fail", "reason": "年线未向上(长线趋势弱)"}
        
    h250, l250 = np.max(highs[-250:]), np.min(lows[-250:])
    
    # 离新高不远（无套牢盘），底部已起飞
    if close < (h250 * 0.85): return {"status": "fail", "reason": "距年内新高>15%"}
    if close < (l250 * 1.30): return {"status": "fail", "reason": "底部脱离不足30%"}
    
    # 防追高
    if close > (ma50 * 1.25): return {"status": "fail", "reason": "偏离50日线>25%(极度超买)"}

    # === VCP 波动率收缩 ===
    recent_highs = highs[-16:-1]
    recent_lows = lows[-16:-1]
    consolidation_depth = (np.max(recent_highs) - np.min(recent_lows)) / np.max(recent_highs)
    
    # 港股放宽一点震幅到 25%（因为港股波动天然更大，没有涨跌幅限制）
    max_depth = 0.20 if market_type == "A" else 0.25
    if consolidation_depth > max_depth: return {"status": "fail", "reason": f"近期平台松散(震幅>{int(max_depth*100)}%)"}

    # === 起爆点侦测 (Pocket Pivot & Thrust) ===
    avg_v50 = np.mean(vols[-50:])
    pct_change_3d = (close - closes[-4]) / closes[-4] if closes[-4] > 0 else 0
    max_vol_3d = np.max(vols[-3:])
    vol_ratio_3d = max_vol_3d / avg_v50 if avg_v50 > 0 else 0
    
    # 必须有实质性的攻击动作和资金入场 (抓腾讯这种巨头起爆的核心)
    if pct_change_3d < 0.05: return {"status": "fail", "reason": "近3日缺乏攻击爆发力(<5%)"}
    if vol_ratio_3d < 1.5: return {"status": "fail", "reason": "近期未见爆发量能(无主力倍量)"}

    # RSI 动量
    deltas = np.diff(closes[-30:])
    up = pd.Series(np.where(deltas > 0, deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
    down = pd.Series(np.where(deltas < 0, -deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
    rsi = 100 - (100 / (1 + (up/down))) if down > 0 else 100
    
    if rsi < 60: return {"status": "fail", "reason": "RSI<60(缺乏主升浪动能)"} 
    
    vol_ratio_today = vols[-1] / avg_v50 if avg_v50 > 0 else 0
    close_60 = closes[-61]
    ret_60 = (close - close_60) / close_60 if close_60 > 0 else 0
    
    data = {
        "Ticker": code, 
        "Name": name, 
        "Price": round(close, 2), 
        "60D_Return%": f"{round(ret_60 * 100, 2)}%",
        "RSI": round(rsi, 2), 
        "Turnover_Rate%": f"{last_turnover}%" if market_type == "A" else "N/A", # 港股忽略换手率展示
        "Vol_Ratio": round(vol_ratio_today, 2),
        "Dist_High%": f"{round(((close - h250) / h250) * 100, 2)}%",
        "Mkt_Cap(亿)": round(mktcap / 100000000, 2), 
        "Turnover(亿)": round(last_amount / 100000000, 2),
        "Trend": "HK Tech/Dividend Breakout" if market_type == "HK" else "Breakout / Accelerating"
    }
    return {"status": "success", "data": data}

def process_single_a_stock(row, session):
    pure_code = str(row['code'])[-6:] 
    name = row['name']
    if pure_code.startswith(('6', '5')): prefix = "1"
    elif pure_code.startswith(('0', '3', '8', '4', '9')): prefix = "0"
    else: return {"status": "fail", "reason": "非A股标的"}
        
    try:
        klines = fetch_kline_data(f"{prefix}.{pure_code}", session)
        if not klines: return {"status": "fail", "reason": "节点阻断"}
        return apply_oneil_logic(pure_code, name, klines, row['mktcap'], "A")
    except Exception: return {"status": "fail", "reason": "解析异常"}

def process_single_hk_stock(row, session):
    pure_code = str(row['code']).zfill(5) # 港股通常是 5 位代码，如 00700
    name = row['name']
    
    try:
        # 东方财富港股 API 前缀通常是 116 (例如 116.00700 是腾讯)
        klines = fetch_kline_data(f"116.{pure_code}", session)
        if not klines: 
            # 兼容个别港股使用 128 前缀的情况
            klines = fetch_kline_data(f"128.{pure_code}", session)
            if not klines: return {"status": "fail", "reason": "节点阻断"}
            
        return apply_oneil_logic(f"{pure_code}.HK", name, klines, row['mktcap'], "HK")
    except Exception: return {"status": "fail", "reason": "解析异常"}


# ==========================================
# 5. 主程序筛选控制 (A股 + 港股)
# ==========================================
def run_screener(market="A"):
    print(f"\n========== 开始处理 {market}股 (盘前盘后全天候防爆版) ==========")
    session = get_robust_session()
    
    if market == "A":
        core_tickers = get_core_tickers_from_sheet(session)
        spot_df = get_eastmoney_market_snapshot(session)
    else:
        core_tickers =[] # 港股市场高度集中，直接全盘扫描流动性最好的即可
        spot_df = get_hk_market_snapshot(session)
        
    if spot_df.empty: return[], f"❌ {market}股大盘数据为空"
    
    total = len(spot_df)
    for col in ['trade', 'prev_close', 'mktcap']: 
        spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce')
    
    spot_df['trade'] = spot_df['trade'].fillna(spot_df['prev_close'])
    spot_df.loc[spot_df['trade'] == 0, 'trade'] = spot_df['prev_close']
    
    if market == "A":
        spot_df['pure_code'] = spot_df['code'].apply(lambda x: str(x)[-6:])
        if core_tickers:
            print(f"🎯 启用主线模式：扫描A股板块提取的 {len(core_tickers)} 只标的。")
            f_df = spot_df[spot_df['pure_code'].isin(core_tickers)].copy()
        else:
            f_df = spot_df.copy()
        # A股过滤规则：价格>10元，市值>50亿人民币
        f_df = f_df[(f_df['trade'] >= 10) & (f_df['mktcap'] >= 5000000000)].copy()
        process_func = process_single_a_stock
        
    else:
        print(f"🌊 启用港股全景模式：深度扫描全部 {total} 只港股！")
        # 🌟 港股过滤核心法则：剔除仙股、剔除小市值老千股
        # 股价 >= 1.0 港元，市值 >= 100亿港元 (完全排除窝轮、牛熊证和垃圾股)
        f_df = spot_df[(spot_df['trade'] >= 1.0) & (spot_df['mktcap'] >= 10000000000)].copy()
        process_func = process_single_hk_stock
        
    print(f"💰 基础过滤完成：剩余 {len(f_df)} 只候选标的！启动并发引擎...")
    
    final_stocks =[]
    fail_reasons = defaultdict(int)

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        futures = {executor.submit(process_func, row, session): row['code'] for _, row in f_df.iterrows()}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res["status"] == "success": final_stocks.append(res["data"])
            elif res["status"] == "fail": fail_reasons[res["reason"]] += 1

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fail_str = "".join([f"   - {r}: {c} 只\n" for r, c in sorted(fail_reasons.items(), key=lambda x:x[1], reverse=True)])
    diag_msg = (
        f"[{now}] {market}股诊断报告：\n"
        f"📊 市场基数: {total}只 | 流动性过滤池: {len(f_df)}只\n"
        f"🏆 最终选出真龙头: {min(len(final_stocks), 50)}只\n"
        f"🔪 深度淘汰明细：\n{fail_str}"
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
            sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
            
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("M1", "Last Updated:")
            sheet.update_acell("N1", now)
            if diag_msg: sheet.update_acell("O1", diag_msg)
            print(f"🎉 成功将 {sheet_name} 前 {len(df)} 只最强龙头写入表格！")
            print(diag_msg)
        else:
            sheet.update_acell("A1", diag_msg if diag_msg else "无符合条件的股票。")
            print(f"⚠️ {sheet_name} 筛选结束，已写入空仓诊断报告。")
            print(diag_msg)
    except Exception as e: 
        print(f"❌ 写入 {sheet_name} 失败: {e}")

if __name__ == "__main__":
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # === 1. 运行 A 股筛选 ===
    try:
        res_a, msg_a = run_screener("A")
        write_to_sheet("A-Share Screener", res_a, "60D_Return%", diag_msg=msg_a)
    except Exception as e:
        error_info = traceback.format_exc()
        write_to_sheet("A-Share Screener",[], "60D_Return%", diag_msg=f"[{now}] A股致命崩溃:\n{error_info}")
        
    # === 2. 运行 港股(HK) 筛选 ===
    try:
        res_hk, msg_hk = run_screener("HK")
        # 注意：必须在你的 Google Sheets 里面提前新建一个叫 "HK-Share Screener" 的工作表！
        write_to_sheet("HK-Share Screener", res_hk, "60D_Return%", diag_msg=msg_hk)
    except Exception as e:
        error_info = traceback.format_exc()
        # 如果报错找不到工作表，会打印在 Github Action 的日志里
        print(f"港股写入失败，请检查是否创建了 'HK-Share Screener' Tab。错误详情: {e}")
