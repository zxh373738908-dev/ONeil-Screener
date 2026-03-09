import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, requests, json, re, time, concurrent.futures, warnings, traceback
from collections import defaultdict
warnings.filterwarnings('ignore')

# ==========================================
# 1. 基础设置与双向 Google Sheets 连接
# ==========================================
SECTOR_SHEET_URL = "https://docs.google.com/spreadsheets/d/1BoYIVL3lb8nZE3U1qAkuO3MTrM117x2qycN1RdrDZgo/edit?gid=0#gid=0"
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

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
        raw_data = []
        sheet_name = ""
        header_row_index = -1
        
        for ws in doc.worksheets():
            data = ws.get_all_values()
            if not data: continue
            for i, row in enumerate(data[:10]):
                row_str = "".join([str(h).upper() for h in row])
                if 'R120' in row_str or 'RANK' in row_str or 'NAME' in row_str:
                    raw_data = data
                    sheet_name = ws.title
                    header_row_index = i
                    break
            if header_row_index != -1: break
                
        if header_row_index == -1:
            print("⚠️ 致命错误：未找到有效表头！")
            return []
            
        print(f"✅ 智能雷达触发：在工作表 [{sheet_name}] 的第 {header_row_index + 1} 行发现了真正的表头！")
        
        headers = [str(h).strip() for h in raw_data[header_row_index]]
        df = pd.DataFrame(raw_data[header_row_index + 1:], columns=headers)
        
        def get_fuzzy_col(keywords, is_pct=True):
            for kw in keywords:
                for col in df.columns:
                    if kw.lower() in str(col).lower().replace(' ', ''):
                        return df[col].apply(parse_pct if is_pct else parse_float)
            return pd.Series(0.0, index=df.index)

        r120 = get_fuzzy_col(['R120', '120日'], True)
        rank = get_fuzzy_col(['Rank', '排名', '强度'], False)
        rel20 = get_fuzzy_col(['REL20'], True)
        rel60 = get_fuzzy_col(['REL60'], True)
        r60 = get_fuzzy_col(['R60', '60日'], True)
        r20 = get_fuzzy_col(['R20', '20日'], True)
        rel5 = get_fuzzy_col(['REL5', '5日'], True)
        
        name_col = df.columns[0]
        for col in df.columns:
            if '名' in col or 'Name' in str(col): 
                name_col = col
                break
            
        print(f"\n🔍 [上帝视角] 数据清洗结果展示 (第一行板块):")
        print(f"板块名称: {df[name_col].iloc[0]} | R120:{r120.iloc[0]}%, Rank:{rank.iloc[0]}, REL20:{rel20.iloc[0]}%, REL5:{rel5.iloc[0]}%")
        
        cond_main = (r120 > 20.0) & (rank >= 80.0) & (rel20 > 0) & (rel60 > 0) & (r60 > 0)
        cond_dip = (r120 > 15.0) & (r20 < 0) & (rel5 > 0)
        
        hot_sectors = df[cond_main][name_col].tolist()
        dip_sectors = df[cond_dip][name_col].tolist()
        
        all_target_sectors = list(set(hot_sectors + dip_sectors))
        print(f"✅ 宏观模型运算完毕：成功锁定 {len(hot_sectors)} 个主线热点板块，{len(dip_sectors)} 个黄金坑板块。")
        
        if all_target_sectors:
            print(f"🎯 提取到的核心板块名单: {', '.join(all_target_sectors)} ...")
            
        return all_target_sectors
    except Exception as e:
        print(f"⚠️ 读取板块表格发生解析错误: {e}")
        return []

# ==========================================
# 3. [第二阶] 锁定板块成分股 (暴力吸干全市场板块库版)
# ==========================================
def get_stocks_from_sectors(sector_names):
    if not sector_names: return set()
    print("\n🧬 [STEP 2] 启动原生 API 引擎提取成分股 (彻底无视防火墙)...")
    target_tickers = set()
    ignore_keywords = ['日经', '纳斯达克', '纳指', '标普', '恒生', '港股', '国债', '债券', '中概']
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    boards_map = {} 
    
    try:
        print("   👉 正在获取全市场板块代码本(突破分页限制)...")
        # 🚀 绝密：把 pz=100 改成了 pz=5000，一次性把东方财富的底裤全部扒光，绝不漏掉一个板块！
        url_ind = "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:2+f:!50&fields=f12,f14"
        url_con = "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:90+t:3+f:!50&fields=f12,f14"
        
        for url in [url_ind, url_con]:
            res = requests.get(url, headers=headers, timeout=5).json()
            if res and 'data' in res and res['data'] and 'diff' in res['data']:
                for item in res['data']['diff']:
                    boards_map[item['f14']] = item['f12'] 
        
        if not boards_map:
            print("🚨 板块字典获取失败！退回全市场扫描。")
            return set()
            
        print(f"   ✅ 成功获取 {len(boards_map)} 个底层板块节点。开始匹配解析...")
        
        for name in sector_names:
            name_str = str(name)
            
            if any(ig in name_str for ig in ignore_keywords):
                print(f"   ⏭️ 过滤非 A 股资产: {name_str}")
                continue
                
            clean_name = re.sub(r'(ETF|LOF|指数|行业|概念|华安|国泰|华泰|柏瑞|广发|易方达|富国|南方|博时|汇添富|嘉实|建信|华夏|银华|天弘|工银|招商|鹏华|联接|泰康|平安|上证|深证|中证|\s*\(.*?\)\s*|\s*（.*?）\s*)', '', name_str)
            clean_name = clean_name.strip()
            if len(clean_name) < 2: clean_name = name_str[:2]
            
            matched_code = None
            matched_name = None
            
            # 优先精确匹配
            for b_name, b_code in boards_map.items():
                if clean_name == b_name:
                    matched_code, matched_name = b_code, b_name
                    break
            
            # 模糊匹配 
            if not matched_code:
                for b_name, b_code in boards_map.items():
                    if clean_name in b_name or b_name in clean_name:
                        matched_code, matched_name = b_code, b_name
                        break
            
            if matched_code:
                print(f"   🔗 匹配成功: [{name_str}] -> 东财板块 [{matched_name}] (代码:{matched_code})")
                cons_url = f"http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=b:{matched_code}&fields=f12"
                cons_res = requests.get(cons_url, headers=headers, timeout=5).json()
                
                if cons_res and 'data' in cons_res and cons_res['data'] and 'diff' in cons_res['data']:
                    tickers = [str(item['f12']).zfill(6) for item in cons_res['data']['diff']]
                    target_tickers.update(tickers)
                    print(f"      📥 成功提取 {len(tickers)} 只成分股")
            else:
                print(f"   ❓ 词库未命中: [{name_str}] (清洗后:[{clean_name}])")
        
        print(f"✅ 成分股提取完毕！共锁定 {len(target_tickers)} 只具备板块效应的核心标的。")
        return target_tickers
        
    except Exception as e:
        print(f"⚠️ 提取成分股原生引擎异常: {e}")
        return set()

# ==========================================
# 4. [第三阶] 新浪基础数据 
# ==========================================
def get_sina_market_snapshot():
    all_data = []
    headers = {'User-Agent': 'Mozilla/5.0'}
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
# 5. [第四阶] 腾讯WEB极速 K 线引擎
# ==========================================
def process_single_stock(row, session):
    pure_code = str(row['code'])[-6:] 
    name = row['name']
    prefix = "sh" if pure_code.startswith(('6', '5')) else "sz"
    
    try:
        k_url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={prefix}{pure_code},day,,,300,qfq"
        res = session.get(k_url, timeout=4).json()
        
        if res.get('code') != 0: return {"status": "fail", "reason": "腾讯WEB接口空"}
            
        data_node = res['data'][f'{prefix}{pure_code}']
        klines = data_node.get('qfqday', data_node.get('day', []))
        
        if len(klines) < 250: return {"status": "fail", "reason": "次新/退市"}
        
        closes = [float(k[2]) for k in klines]
        highs = [float(k[3]) for k in klines]
        lows = [float(k[4]) for k in klines]
        vols = [float(k[5]) for k in klines]
        
        cs, hs, ls, vs = pd.Series(closes), pd.Series(highs), pd.Series(lows), pd.Series(vols)
        close, close_60 = cs.iloc[-1], cs.iloc[-61]
        if close == 0.0: return {"status": "fail", "reason": "停牌"}
        
        ma20, ma50, ma150, ma200 = cs.rolling(20).mean().iloc[-1], cs.rolling(50).mean().iloc[-1], cs.rolling(150).mean().iloc[-1], cs.rolling(200).mean().iloc[-1]
        if not (close > ma50 and ma50 > ma150 and ma150 > ma200): 
            return {"status": "fail", "reason": "非多头排列"}
            
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
        return {"status": "success", "data": data, "log": f"✅ 捕获主升浪龙头: {pure_code} {name}"}
    except Exception as e: 
        return {"status": "fail", "reason": "接口报错"}

# ==========================================
# 6. 主程序流转控制
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股 (完美降维打击版) ==========")
    
    target_sectors = get_target_sectors()
    if not target_sectors:
        return [], "[系统保护] 宏观大盘无符合条件的热门板块，严格执行空仓纪律！"
        
    core_tickers = get_stocks_from_sectors(target_sectors)
    if not core_tickers:
        return [], "❌ 虽然有主线，但提取成分股失败，请检查网络或稍后重试。"
    
    print("\n📊 [STEP 3] 扫描全市场流动性...")
    spot_df = get_sina_market_snapshot()
    if spot_df.empty: return [], "❌ 大盘数据为空"
    
    for col in ['trade','mktcap','amount','turnoverratio']: spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce')
    spot_df['mktcap'] *= 10000
    
    spot_df['pure_code'] = spot_df['code'].apply(lambda x: str(x)[-6:])
    f_df = spot_df[spot_df['pure_code'].isin(core_tickers)].copy()
    print(f"🎯 板块降维完成：全市场 5000 只股票 -> 目标池瞬间骤降至 {len(f_df)} 只板块内核心股。")
