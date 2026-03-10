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

# 🌟【超级防护服】：建立全局 Session，带连接池与底层自动重试，伪装级别拉满
def get_robust_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
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
# 2. 板块宏观模型 (修复底层API参数陷阱 + 双重保险映射)
# ==========================================
def get_core_tickers_from_sheet(session):
    print("\n🌍[STEP 1] 尝试连接宏观大盘，寻找热点板块...")
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
            
        headers =[]
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
        
        # 🌟【修复点1】去除了导致失败的 +f:!50 参数，确保行业板块完整下载！
        boards_map = {}
        for url in[
            "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2&fields=f12,f14", # 行业板块
            "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:3&fields=f12,f14"  # 概念板块
        ]:
            for _ in range(3):
                try:
                    res = session.get(url, timeout=5).json()
                    if res and 'data' in res and res['data']:
                        for item in res['data']['diff']: boards_map[item['f14']] = item['f12']
                    break
                except: time.sleep(1)
                
        target_tickers = set()
        
        # 🌟 智能同义词映射库 (扩大覆盖面)
        synonyms = {
            "航空航天":["航天航空", "大飞机", "卫星通信", "国防军工", "军工"],
            "有色金属":["小金属", "工业金属", "能源金属", "稀缺资源", "基本金属"],
            "黄金":["贵金属", "黄金概念", "珠宝首饰"],
            "煤炭":["煤炭行业", "煤炭概念"],
            "光伏":["光伏设备", "光伏概念", "太阳能", "BC电池"],
            "新能源车": ["汽车整车", "汽车零部件", "新能源车概念"],
            "新能源":["风电设备", "光伏设备", "电池", "绿色电力"],
            "传媒":["文化传媒", "游戏", "短剧互动游戏"],
            "芯片":["半导体", "芯片概念", "存储芯片"],
            "医药":["化学制药", "中药", "生物制品", "医药商业", "医疗器械", "创新药"],
            "军工":["航天航空", "船舶制造", "兵器装备", "军工概念"],
            "通信":["通信设备", "通信服务", "5G概念", "6G概念", "CPO概念"],
            "软件":["软件开发", "IT服务", "信创"]
        }
        
        # 🌟【修复点2】核弹级兜底：直接注入东方财富底层 BK 代码，保证绝不抓空！
        hardcoded_bk = {
            "黄金":["BK0477", "BK0717"], 
            "煤炭":["BK0437", "BK0532"], 
            "有色金属":["BK0478", "BK0479", "BK0496"], 
            "光伏":["BK1031", "BK0854"], 
            "航空航天":["BK0480", "BK0498"]
        }
        
        overseas_broad_keywords =['日经', '纳指', '标普', '恒生', '港股', '德国', '法国', '亚洲', '中概', '中证', '沪深', '上证', '深证', '科创', '创业板50', '双创', '红利低波']

        for etf_name in target_etfs:
            if any(k in str(etf_name).upper() for k in overseas_broad_keywords):
                print(f"   -> ⏭️ [{etf_name}] 属于跨境/宽基指数，跳过A股映射")
                continue

            # 核心词提取
            clean_name = re.sub(r'(ETF|LOF|指数|基金|增强|发起式|联接|A|C|类).*$', '', str(etf_name), flags=re.IGNORECASE).strip()
            if not clean_name: continue
            
            # 1. 尝试同义词模糊匹配
            search_terms = set([clean_name])
            for key, aliases in synonyms.items():
                if key in clean_name or clean_name in key:
                    search_terms.update(aliases)
            
            matched_b_codes = set()
            for term in search_terms:
                for b_name, b_code in boards_map.items():
                    if term in b_name or b_name in term:
                        matched_b_codes.add(b_code)
                        
            # 2. 触发核弹兜底机制 (针对老是匹配失败的顽固分子)
            for key, bks in hardcoded_bk.items():
                if key in clean_name or clean_name in key:
                    matched_b_codes.update(bks)
            
            if matched_b_codes:
                print(f"   -> ✅ [{etf_name}] (关键词:{clean_name}) 映射成功: {list(matched_b_codes)}")
                for b_code in matched_b_codes:
                    for _ in range(3):
                        try:
                            list_url = f"https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=b:{b_code}&fields=f12"
                            cons = session.get(list_url, timeout=5).json()
                            if cons and 'data' in cons and cons['data'] and 'diff' in cons['data']:
                                target_tickers.update([str(i['f12']).zfill(6) for i in cons['data']['diff']])
                            break
                        except: time.sleep(1)
            else:
                print(f"   -> ⚠️ [{etf_name}] (关键词:{clean_name}) 未能匹配到任何A股板块")
                
        if not target_tickers: raise Exception("所有热点ETF均未能匹配到A股成分股")
        return list(target_tickers)
        
    except Exception as e:
        print(f"⚠️ 板块宏观筛选未能生效 ({type(e).__name__}: {str(e)})。系统将自动降级为【全市场扫描】！")
        return
        
# ==========================================
# 3. 东方财富大盘扫描器 (替代不稳定的新浪API)
# ==========================================
def get_eastmoney_market_snapshot(session):
    print("🚀 启动【东方财富】抓取全市场基础代码库 (极速全量+盘前昨收价)...")
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1", "pz": "6000", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": "2", "invt": "2",
        "fid": "f3", "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
        # f2=最新价, f18=昨收价(盘前兜底关键), f20=总市值
        "fields": "f12,f14,f2,f18,f20"
    }
    for _ in range(3):
        try:
            res = session.get(url, params=params, timeout=10).json()
            if res and 'data' in res and res['data'] and 'diff' in res['data']:
                df = pd.DataFrame(res['data']['diff'])
                df.rename(columns={'f12': 'code', 'f14': 'name', 'f2': 'trade', 'f18': 'prev_close', 'f20': 'mktcap'}, inplace=True)
                return df
        except Exception:
            time.sleep(1)
    return pd.DataFrame()

# ==========================================
# 4. K线运算与底层加速
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

def process_single_stock(row, session):
    pure_code = str(row['code'])[-6:] 
    name = row['name']
    
    # 增加北交所前缀兼容处理(8,4,9)
    if pure_code.startswith(('6', '5')): prefix = "1"
    elif pure_code.startswith(('0', '3', '8', '4', '9')): prefix = "0"
    else: return {"status": "fail", "reason": "非A股标的"}
        
    try:
        klines = fetch_kline_data(f"{prefix}.{pure_code}", session)
        if not klines: return {"status": "fail", "reason": "节点阻断"}
        
        valid_klines = [k.split(',') for k in klines if len(k.split(',')) >= 8]
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
        
        # 欧奈尔底线条件：流动性不能太差
        if last_amount < 200000000: return {"status": "fail", "reason": "成交额<2亿"}
        if last_turnover < 1.5: return {"status": "fail", "reason": "换手率<1.5%"}

        close = closes[-1]
        if close == 0.0: return {"status": "fail", "reason": "停牌"}
        
        # ==========================================
        # 🌟 欧奈尔/米尔维尼 核心趋势模板 (Trend Template)
        # ==========================================
        ma20 = np.mean(closes[-20:])
        ma50 = np.mean(closes[-50:])
        ma150 = np.mean(closes[-150:])
        ma200 = np.mean(closes[-200:])
        ma200_20d_ago = np.mean(closes[-220:-20]) # 20天前的200日均线
        
        # 1. 严格的多头排列
        if not (close > ma20 and close > ma50 and ma50 > ma150 and ma150 > ma200): 
            return {"status": "fail", "reason": "非标准多头排列"}
            
        # 2. 200日均线必须是向上的（过滤死猫反弹）
        if ma200 < ma200_20d_ago:
            return {"status": "fail", "reason": "年线未向上(长线趋势弱)"}
            
        # ==========================================
        # 🌟 价格位置与动能 (Proximity & Momentum)
        # ==========================================
        h250, l250 = np.max(highs[-250:]), np.min(lows[-250:])
        
        # 3. 欧奈尔选股精髓：离一年新高不能太远（上方无套牢盘，最容易拉升）
        if close < (h250 * 0.85): return {"status": "fail", "reason": "距年内新高>15%"}
        if close < (l250 * 1.30): return {"status": "fail", "reason": "底部脱离不足30%"}
        
        # 4. 防追高机制：不能偏离50日线太远，过滤已经在天上、随时见顶回落的票
        if close > (ma50 * 1.25): return {"status": "fail", "reason": "偏离50日线>25%(极度超买)"}

        # ==========================================
        # 🌟 VCP 波动率收缩 (爆发前的蓄力)
        # ==========================================
        # 检查过去15天（洗盘期）的振幅，形态必须紧凑，不能是大起大落的散户盘
        recent_highs = highs[-16:-1]
        recent_lows = lows[-16:-1]
        consolidation_depth = (np.max(recent_highs) - np.min(recent_lows)) / np.max(recent_highs)
        if consolidation_depth > 0.20: return {"status": "fail", "reason": "近期平台松散(震幅>20%)"}

        # ==========================================
        # 🌟 起爆点/加速点侦测 (Pocket Pivot & Thrust)
        # ==========================================
        avg_v50 = np.mean(vols[-50:])
        
        # 侦测近3日的极限爆发力（包含今天刚启动，或者前天启动今天正在加速的）
        pct_change_3d = (close - closes[-4]) / closes[-4] if closes[-4] > 0 else 0
        max_vol_3d = np.max(vols[-3:])
        vol_ratio_3d = max_vol_3d / avg_v50 if avg_v50 > 0 else 0
        
        # 5. 必须有实质性的攻击动作和资金入场
        if pct_change_3d < 0.05: return {"status": "fail", "reason": "近3日缺乏攻击爆发力(<5%)"}
        if vol_ratio_3d < 1.5: return {"status": "fail", "reason": "近期未见爆发量能(无主力倍量)"}

        # RSI 动量确认
        deltas = np.diff(closes[-30:])
        up = pd.Series(np.where(deltas > 0, deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
        down = pd.Series(np.where(deltas < 0, -deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
        rsi = 100 - (100 / (1 + (up/down))) if down > 0 else 100
        
        if rsi < 60: return {"status": "fail", "reason": "RSI<60(缺乏主升浪动能)"} # 欧奈尔选股RSI要求极高
        
        # 当日量比
        vol_ratio_today = vols[-1] / avg_v50 if avg_v50 > 0 else 0
        
        # 计算60日涨幅（供表格排序使用）
        close_60 = closes[-61]
        ret_60 = (close - close_60) / close_60 if close_60 > 0 else 0
        
        data = {
            "Ticker": pure_code, 
            "Name": name, 
            "Price": round(close, 2), 
            "60D_Return%": f"{round(ret_60 * 100, 2)}%",
            "RSI": round(rsi, 2), 
            "Turnover_Rate%": f"{last_turnover}%", 
            "Vol_Ratio": round(vol_ratio_today, 2),
            "Dist_High%": f"{round(((close - h250) / h250) * 100, 2)}%",
            "Mkt_Cap(亿)": round(row['mktcap'] / 100000000, 2), 
            "Turnover(亿)": round(last_amount / 100000000, 2),
            "Trend": "Breakout / Accelerating" # 状态更新为突破/加速
        }
        return {"status": "success", "data": data}
        
    except Exception as e: 
        return {"status": "fail", "reason": "解析异常"}

# ==========================================
# 5. 主程序控制
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股 (盘前盘后全天候防爆版) ==========")
    
    session = get_robust_session()
    core_tickers = get_core_tickers_from_sheet(session)
    
    spot_df = get_eastmoney_market_snapshot(session)
    if spot_df.empty: return[], "❌ 大盘数据为空"
    
    total = len(spot_df)
    
    # 强制转换数值，无法转换的变为 NaN
    for col in['trade', 'prev_close', 'mktcap']: 
        spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce')
    
    # 🌟 盘前自适应神技：如果最新价(trade)是 NaN 或 0 (盘前/深夜无价格)，则用昨收价无缝填补！
    spot_df['trade'] = spot_df['trade'].fillna(spot_df['prev_close'])
    spot_df.loc[spot_df['trade'] == 0, 'trade'] = spot_df['prev_close']
    
    # (已剔除 spot_df['mktcap'] *= 10000 错误单位放大)
    spot_df['pure_code'] = spot_df['code'].apply(lambda x: str(x)[-6:])
    
    if core_tickers:
        print(f"🎯 启用主线模式：将仅扫描板块提取出的 {len(core_tickers)} 只标的。")
        f_df = spot_df[spot_df['pure_code'].isin(core_tickers)].copy()
    else:
        print(f"🌊 启用全市场扫描模式：对全部 {total} 只A股进行清洗！")
        f_df = spot_df.copy()
        
    f_df = f_df[(f_df['trade'] >= 10) & (f_df['mktcap'] >= 5000000000)].copy()
    
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
        write_to_sheet("A-Share Screener",[], "60D_Return%", diag_msg=f"[{now}] 致命崩溃:\n{error_info}")
