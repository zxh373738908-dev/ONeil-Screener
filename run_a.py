import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, requests, json, re, concurrent.futures, warnings, traceback, random, time
from collections import defaultdict
import io
import urllib.parse

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

# ==========================================
# 🛡️ 核心武器：智能代理中转网络 (彻底无视 IP 封锁)
# ==========================================
class SmartSession:
    def __init__(self):
        self.use_proxy = False # 全局封锁降级标记

    def get_json(self, url, params=None):
        if params:
            query = urllib.parse.urlencode(params)
            full_url = url + ("&" if "?" in url else "?") + query
        else:
            full_url = url

        # 🚀 缓存击穿：强制加入随机毫秒级时间戳，防止代理服务器返回旧数据
        rnd = str(int(time.time() * 1000) + random.randint(1, 10000))
        full_url += f"&_rnd={rnd}" if "?" in full_url else f"?_rnd={rnd}"
        encoded_url = urllib.parse.quote(full_url, safe='')

        # 代理服务列表 (全球顶级免费中转站)
        proxy_list =[
            {"url": f"https://api.codetabs.com/v1/proxy?quest={full_url}", "type": "raw"},
            {"url": f"https://api.allorigins.win/get?url={encoded_url}", "type": "json_contents"},
            {"url": f"https://corsproxy.io/?{encoded_url}", "type": "raw"},
            {"url": f"https://api.allorigins.win/raw?url={encoded_url}", "type": "raw"}
        ]
        
        # 如果已经被判定为封锁状态，自动将代理列表打乱，实现【负载均衡】，防止单一代理被挤爆
        if self.use_proxy:
            random.shuffle(proxy_list)
            proxies = proxy_list
        else:
            proxies = [{"url": full_url, "type": "direct"}] + proxy_list

        for p in proxies:
            try:
                # 伪装普通人，不带任何刺眼的爬虫特征
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
                if p["type"] == "direct":
                    headers['Referer'] = 'https://quote.eastmoney.com/'
                    
                # 不使用会产生冲突的全局连接池，独立短连接，绝对安全
                res = requests.get(p["url"], headers=headers, timeout=8)
                
                if res.status_code == 200:
                    if p["type"] == "json_contents":
                        raw_data = res.json().get('contents')
                        if not raw_data: continue
                        data = json.loads(raw_data)
                    else:
                        data = res.json()
                        
                    if data and 'data' in data:
                        # 💥 战术通报：如果直连被封后首次由代理抢通，全网广播！
                        if p["type"] != "direct" and not self.use_proxy:
                            self.use_proxy = True
                            print(f"\n   -> 🌐 警报：检测到 Github IP 被东财墙杀！系统已自动启动【全球代理中转网络】进行火力压制！")
                        return data
                        
            except Exception:
                if p["type"] == "direct":
                    self.use_proxy = True
                continue
                
        return None

# 初始化单例中转器
smart_client = SmartSession()

# ==========================================
# 2. 板块宏观模型
# ==========================================
def get_core_tickers_from_sheet():
    print("\n🌍[STEP 1] 正在同步 Google Sheets 宏观大盘，寻找热点板块...")
    try:
        csv_url = SECTOR_SHEET_URL.replace("/edit?", "/export?format=csv&").replace("#gid=", "&gid=")
        try:
            # Google Sheets 依然用普通的 requests 直连，因为它不会封 Github Actions
            res = requests.get(csv_url, timeout=10)
            res.raise_for_status()
            raw_df = pd.read_csv(io.StringIO(res.text), header=None)
        except Exception:
            print(f"   -> ⚠️ CSV 快读受阻，自动降级使用 API 读取...")
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
        
        if not target_etfs:
            print("   -> ⚠️ 当前市场无符合条件的长线热点板块。")
            return[]

        print(f"   -> ✅ 成功锁定 {len(target_etfs)} 个热点ETF，正在映射 A 股成分股...")
        boards_map = {}
        for url in[
            "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2&fields=f12,f14",
            "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:3&fields=f12,f14"
        ]:
            data = smart_client.get_json(url)
            if data and 'data' in data and data['data']:
                for item in data['data']['diff']: boards_map[item['f14']] = item['f12']
                
        target_tickers = set()
        synonyms = {"化工":["化工行业", "磷化工", "煤化工", "基础化工", "化肥行业"]} 
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
                list_url = f"https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=b:{b_code}&fields=f12"
                cons = smart_client.get_json(list_url)
                if cons and 'data' in cons and cons['data'] and 'diff' in cons['data']:
                    target_tickers.update([str(i['f12']).zfill(6) for i in cons['data']['diff']])
        
        print(f"   -> 🎯 板块映射完成，共提取 {len(target_tickers)} 只主线标的！")
        return list(target_tickers)
    except Exception as e: 
        print(f"   -> ⚠️ 板块读取遇到阻碍 ({str(e)})，系统将自动切入全市场盲扫！")
        return[]

# ==========================================
# 3. 大盘扫描器
# ==========================================
def get_eastmoney_market_snapshot():
    print("\n🚀 [STEP 2] 启动【东方财富】底层引擎：抓取全市场 A 股快照...")
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1", "pz": "6000", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": "2", "invt": "2",
        "fid": "f3", "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
        "fields": "f12,f14,f2,f18,f20"
    }
    
    data = smart_client.get_json(url, params=params)
    if data and 'data' in data and data['data'] and 'diff' in data['data']:
        df = pd.DataFrame(data['data']['diff'])
        df.rename(columns={'f12': 'code', 'f14': 'name', 'f2': 'trade', 'f18': 'prev_close', 'f20': 'mktcap'}, inplace=True)
        print(f"   -> ✅ 数据通道连接成功！抓取全市场 {len(df)} 只股票基础数据！")
        return df
        
    print("   -> ❌ 致命错误：大盘基础数据抓取失败！")
    return pd.DataFrame()

# ==========================================
# 4. 战术分析：神奇九转 TD9 底层引擎
# ==========================================
def check_td9_or_oversold(closes):
    for offset in range(5):
        idx = len(closes) - 1 - offset
        if idx >= 13:
            if all(closes[idx-i] < closes[idx-i-4] for i in range(9)):
                return True
                
    deltas = np.diff(closes[-30:])
    up = pd.Series(np.where(deltas > 0, deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
    down = pd.Series(np.where(deltas < 0, -deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
    rsi = 100 - (100 / (1 + (up/down))) if down > 0 else 100
    if rsi < 35: return True
    return False

# ==========================================
# 5. K线运算与【三大主力战法】逻辑
# ==========================================
def fetch_kline_data(secid):
    url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f61&klt=101&fqt=1&end=20500000&lmt=300"
    data = smart_client.get_json(url)
    if data and 'data' in data and data['data'] and 'klines' in data['data']:
        return data['data']['klines']
    return None

def process_single_stock(row):
    pure_code = str(row['code'])[-6:] 
    name = row['name']
    
    if pure_code.startswith(('6', '5')): prefix = "1"
    elif pure_code.startswith(('0', '3', '8', '4', '9')): prefix = "0"
    else: return {"status": "fail", "reason": "非A股"}
        
    try:
        klines = fetch_kline_data(f"{prefix}.{pure_code}")
        if not klines: return {"status": "fail", "reason": "无K线"}
        
        valid_klines =[k.split(',') for k in klines if len(k.split(',')) >= 8]
        
        # 🛡️ 盘前 0 交易量自动清理，完美防早盘被错杀
        while len(valid_klines) > 0:
            try:
                vol = float(valid_klines[-1][5])
                if vol == 0: valid_klines.pop()
                else: break
            except: valid_klines.pop()
            
        if len(valid_klines) < 250: return {"status": "fail", "reason": "次新/退市"}

        k_matrix = np.array(valid_klines)
        closes = k_matrix[:, 2].astype(float)
        highs = k_matrix[:, 3].astype(float)
        lows = k_matrix[:, 4].astype(float)
        vols = k_matrix[:, 5].astype(float)
        amounts = k_matrix[:, 6].astype(float) 
        turnovers = k_matrix[:, 7].astype(float) 

        # 五日均量防止早盘错杀
        avg_amount_5 = np.mean(amounts[-5:])
        avg_turnover_5 = np.mean(turnovers[-5:])
        close = closes[-1]
        
        if avg_amount_5 < 150000000: return {"status": "fail", "reason": "五日均额<1.5亿"}
        if avg_turnover_5 < 0.6 and avg_amount_5 < 300000000: return {"status": "fail", "reason": "流动性不足"}

        ma20 = np.mean(closes[-20:])
        ma50 = np.mean(closes[-50:])
        ma60 = np.mean(closes[-60:])  
        ma150 = np.mean(closes[-150:])
        ma200 = np.mean(closes[-200:])
        
        h250 = np.max(highs[-250:])
        h60 = np.max(highs[-60:])

        avg_v50 = np.mean(vols[-50:])
        pct_change_today = (close - closes[-2]) / closes[-2] if closes[-2] > 0 else 0
        pct_change_3d = (close - closes[-4]) / closes[-4] if closes[-4] > 0 else 0
        vol_ratio_today = vols[-1] / avg_v50 if avg_v50 > 0 else 0

        deltas = np.diff(closes[-30:])
        up = pd.Series(np.where(deltas > 0, deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
        down = pd.Series(np.where(deltas < 0, -deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
        rsi = 100 - (100 / (1 + (up/down))) if down > 0 else 100

        # ==========================================
        # 🌟 三大主力战法引擎 
        # ==========================================
        is_breakout = (
            close > ma20 and close > ma50 and 
            pct_change_3d >= 0.045 and 
            np.max(vols[-3:]) / avg_v50 >= 1.5 and 
            rsi >= 60 and 
            (ma50 > ma150 and ma150 > ma200) 
        )
        
        is_ambush = (
            ((abs(close - ma20)/ma20 < 0.04) or (abs(close - ma50)/ma50 < 0.04)) and 
            vol_ratio_today < 1.1 and 
            -0.05 < pct_change_3d < 0.04 and 
            close >= ma50 * 0.98 and
            (ma50 > ma150 and ma150 > ma200)
        )

        mktcap_val = float(row['mktcap']) if pd.notna(row['mktcap']) else 0
        
        # 🐉 Engine C：专抓黄金坑/老龙回头
        c_large_cap = mktcap_val >= 10000000000
        c_ret_120 = (close - closes[-121]) / closes[-121] > 0.15
        c_dist_h60 = (close - h60) / h60
        c_golden_pit = -0.25 <= c_dist_h60 <= -0.05 
        c_support = close >= ma60 * 0.98 and (closes[-2] < ma20 or close < ma20 * 1.03)
        c_ignite = (pct_change_today > 0.03 and vol_ratio_today > 1.5) or check_td9_or_oversold(closes)

        is_golden_pit_dragon = c_large_cap and c_ret_120 and c_golden_pit and c_support and c_ignite

        if not (is_breakout or is_ambush or is_golden_pit_dragon):
            return {"status": "fail", "reason": "未达战法标准"}

        if is_golden_pit_dragon:
            trend_status = "🐉 黄金坑反转(午后定音)"
        elif is_breakout:
            trend_status = "🔥 右侧突破起飞"
        else:
            trend_status = "🧘‍♂️ 左侧缩量伏击(踩均线)"

        ret_60 = (close - closes[-61]) / closes[-61] if closes[-61] > 0 else 0
        data = {
            "Ticker": pure_code, 
            "Name": name, 
            "Price": round(close, 2), 
            "Type": trend_status,  
            "60D_Return%": f"{round(ret_60 * 100, 2)}%",
            "RSI": round(rsi, 2), 
            "Turnover_Rate%": f"{turnovers[-1]}%", 
            "Vol_Ratio": round(vol_ratio_today, 2),
            "Dist_High%": f"{round(((close - h250) / h250) * 100, 2)}%",
            "Mkt_Cap(亿)": round(mktcap_val / 100000000, 2), 
            "Turnover(亿)": round(amounts[-1] / 100000000, 2)
        }
        return {"status": "success", "data": data}
        
    except Exception as e: 
        return {"status": "fail", "reason": "解析异常"}

# ==========================================
# 6. 主程序控制
# ==========================================
def screen_a_shares():
    print("\n========== A股 猎手三引擎版 (全球代理破壁版) ==========")
    
    core_tickers = get_core_tickers_from_sheet()
    spot_df = get_eastmoney_market_snapshot()
    
    if spot_df.empty: 
        return[], "❌ 战略终止：大盘数据为空"
    
    total = len(spot_df)
    for col in['trade', 'prev_close', 'mktcap']: 
        spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce')
    
    spot_df['trade'] = spot_df['trade'].fillna(spot_df['prev_close'])
    spot_df.loc[spot_df['trade'] == 0, 'trade'] = spot_df['prev_close']
    spot_df['mktcap'] = spot_df['mktcap'].fillna(0)
    
    spot_df['pure_code'] = spot_df['code'].apply(lambda x: str(x)[-6:])
    
    if core_tickers:
        print(f"\n🎯 [STEP 3] 启用【主线狙击】模式：专注扫描 {len(core_tickers)} 只标的！")
        f_df = spot_df[spot_df['pure_code'].isin(core_tickers)].copy()
    else:
        print(f"\n🌊 [STEP 3] 启用【全景扫雷】模式：对全市场 {total} 只股票进行清洗！")
        f_df = spot_df.copy()
        
    f_df = f_df[(f_df['trade'] >= 5) & (f_df['mktcap'] >= 4000000000)].copy()
    print(f"   -> 💰 剔除极小盘和仙股后，剩余 {len(f_df)} 只候选标的！正在全速并发演算...")
    
    final_stocks =[]
    fail_reasons = defaultdict(int)

    # 🛡️ 智能降速防拥堵：如果走了代理中转，我们将并发从 12 降到 4，防止拥堵免费代理服务器引发 429 报错
    max_w = 4 if smart_client.use_proxy else 12
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as executor:
        futures = {executor.submit(process_single_stock, row): row['code'] for _, row in f_df.iterrows()}
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
    print("\n📝 [STEP 4] 正在将绝密作战名单写入 Google Sheets 表格...")
    try:
        sheet = client.open_by_url(OUTPUT_SHEET_URL).worksheet(sheet_name)
        sheet.clear()
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            df['Sort_Num'] = df[sort_col].str.replace('%', '').astype(float)
            df = df.sort_values(by='Sort_Num', ascending=False).drop(columns=['Sort_Num'])
            df = df.head(50) 
            cols =['Ticker', 'Name', 'Price', 'Type', '60D_Return%', 'RSI', 'Turnover_Rate%', 'Vol_Ratio', 'Dist_High%', 'Mkt_Cap(亿)', 'Turnover(亿)']
            df = df[cols]
            sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
            
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("M1", "Last Updated:")
            sheet.update_acell("N1", now)
            if diag_msg: sheet.update_acell("O1", diag_msg)
            print(f"🎉 大功告成！已成功将 {len(df)} 只最强龙头送达指挥部！")
            print(f"\n{diag_msg}")
        else:
            sheet.update_acell("A1", diag_msg if diag_msg else "无符合条件的股票。")
            print("⚠️ 筛选完毕：当前战局恶劣，未发现任何符合狙击条件的标的。已写入空仓诊断报告！")
            print(f"\n{diag_msg}")
    except Exception as e: 
        print(f"❌ 写入失败，原因: {e}")

if __name__ == "__main__":
    try:
        res, msg = screen_a_shares()
        write_to_sheet("A-Share Screener", res, "60D_Return%", diag_msg=msg)
    except Exception as e:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_info = traceback.format_exc()
        write_to_sheet("A-Share Screener",[], "60D_Return%", diag_msg=f"[{now}] 致命崩溃:\n{error_info}")
