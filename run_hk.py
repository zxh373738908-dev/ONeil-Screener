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
SECTOR_SHEET_URL = "https://docs.google.com/spreadsheets/d/17avi7qslnc_bCVhxvAYLRdwbFFAcAsqi45x2Gd4bQZc/edit?gid=0#gid=0"
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
# 2. 板块宏观模型 (兼容 120R / R120)
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
            # 优化：兼容寻找 120R 或 R120
            if '120R' in row_str or 'R120' in row_str:
                header_idx = i; break
                
        if header_idx == -1: raise Exception("未在表格中找到包含 120R 或 R120 的表头行")
            
        headers =[]
        for h in raw_df.iloc[header_idx].values:
            h_str = str(h).strip()
            if not h_str or h_str in headers:
                headers.append(f"Unnamed_{len(headers)}")
            else:
                headers.append(h_str)
                
        df = pd.DataFrame(raw_df.iloc[header_idx + 1:].values, columns=headers)
        
        name_col = next((c for c in df.columns if '名' in c or 'Name' in str(c)), None)
        # 优化：正确匹配 120R 或 60R
        r120_col = next((c for c in df.columns if '120R' in c.upper() or 'R120' in c.upper()), None)
        r60_col = next((c for c in df.columns if '60R' in c.upper() or 'R60' in c.upper()), None)
        
        if not name_col or not r120_col or not r60_col:
            raise Exception("缺失必要列(Name / 120R / 60R)，请检查表格")
            
        r120 = df[r120_col].apply(lambda x: parse_val(x, True))
        r60 = df[r60_col].apply(lambda x: parse_val(x, True))
        
        target_etfs = df[(r120 > 20.0) & (r60 > 0)][name_col].tolist()
        if not target_etfs: raise Exception("无符合 120R>20% 且 60R>0 的热门板块")
        
        # 缩略了A股映射逻辑（为了专注港股，如果作为辅助扫描）...
        return[]
        
    except Exception as e:
        print(f"⚠️ 板块宏观筛选未能生效 ({type(e).__name__}: {str(e)})。已降级为全市场扫描！")
        return[]

# ==========================================
# 3. 市场大盘数据获取 (重点修复港股截断 Bug)
# ==========================================
def get_hk_market_snapshot(session):
    print("🚀 启动【东方财富】抓取 港股(HK) 全市场基础代码库...")
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    all_data =[]
    page = 1
    # 🌟 修复: 之前被限流100条，现在改用分页循环暴力抓取全部 ~2600 只港股！
    while True:
        params = {
            "pn": str(page), "pz": "500", "po": "1", "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": "2", "invt": "2",
            "fid": "f3", "fs": "m:116+t:3,m:116+t:4",  # 港股主板的核心标志
            "fields": "f12,f14,f2,f18,f20"
        }
        try:
            res = session.get(url, params=params, timeout=10).json()
            if not res or 'data' not in res or not res['data'] or not res['data'].get('diff'):
                break
            diff = res['data']['diff']
            all_data.extend(diff)
            if len(diff) < 500: break  # 达到最后一页
            page += 1
        except Exception:
            time.sleep(1)
            
    df = pd.DataFrame(all_data)
    if not df.empty:
        df.rename(columns={'f12': 'code', 'f14': 'name', 'f2': 'trade', 'f18': 'prev_close', 'f20': 'mktcap'}, inplace=True)
    return df

# ==========================================
# 4. K线核心引擎 (专为港股波动率调优)
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

def apply_oneil_logic(code, name, klines, mktcap, market_type="HK"):
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
    
    # 港股流动性防爆墙：卡绝对成交额（>5000万港币），因腾讯等巨头换手率可能很低
    if market_type == "HK":
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
    
    # 🌟 港股波动容忍度调优：放宽到距新高 20% 以内（原15%），底部脱离 25%（原30%）
    dist_high_limit = 0.80 if market_type == "HK" else 0.85
    dist_low_limit = 1.25 if market_type == "HK" else 1.30
    if close < (h250 * dist_high_limit): return {"status": "fail", "reason": f"距年内新高>{int((1-dist_high_limit)*100)}%"}
    if close < (l250 * dist_low_limit): return {"status": "fail", "reason": f"底部脱离不足{int((dist_low_limit-1)*100)}%"}
    
    # 防追高
    max_ma50_dev = 1.30 if market_type == "HK" else 1.25
    if close > (ma50 * max_ma50_dev): return {"status": "fail", "reason": f"偏离50日线>{int((max_ma50_dev-1)*100)}%(极度超买)"}

    # === VCP 波动率收缩 ===
    recent_highs = highs[-16:-1]
    recent_lows = lows[-16:-1]
    consolidation_depth = (np.max(recent_highs) - np.min(recent_lows)) / np.max(recent_highs)
    
    max_depth = 0.25 if market_type == "HK" else 0.20
    if consolidation_depth > max_depth: return {"status": "fail", "reason": f"近期平台松散(震幅>{int(max_depth*100)}%)"}

    # === 起爆点侦测 (Pocket Pivot & Thrust) ===
    avg_v50 = np.mean(vols[-50:])
    pct_change_3d = (close - closes[-4]) / closes[-4] if closes[-4] > 0 else 0
    max_vol_3d = np.max(vols[-3:])
    vol_ratio_3d = max_vol_3d / avg_v50 if avg_v50 > 0 else 0
    
    # 🌟 港股爆发力调优：3日涨幅放宽至 >4%，给大盘股空间
    min_3d_pct = 0.04 if market_type == "HK" else 0.05
    if pct_change_3d < min_3d_pct: return {"status": "fail", "reason": f"近3日缺乏攻击爆发力(<{int(min_3d_pct*100)}%)"}
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
        "Turnover_Rate%": "N/A", 
        "Vol_Ratio": round(vol_ratio_today, 2),
        "Dist_High%": f"{round(((close - h250) / h250) * 100, 2)}%",
        "Mkt_Cap(亿)": round(mktcap / 100000000, 2), 
        "Turnover(亿)": round(last_amount / 100000000, 2),
        "Trend": "HK Tech/Dividend Breakout"
    }
    return {"status": "success", "data": data}


def process_single_hk_stock(row, session):
    pure_code = str(row['code']).zfill(5)
    name = row['name']
    
    try:
        # 东方财富港股 API 前缀
        klines = fetch_kline_data(f"116.{pure_code}", session)
        if not klines: 
            klines = fetch_kline_data(f"128.{pure_code}", session)
            if not klines: return {"status": "fail", "reason": "节点阻断"}
            
        return apply_oneil_logic(f"{pure_code}.HK", name, klines, row['mktcap'], "HK")
    except Exception: return {"status": "fail", "reason": "解析异常"}


# ==========================================
# 5. 主程序筛选控制
# ==========================================
def run_screener(market="HK"):
    print(f"\n========== 开始处理 {market}股 (深度扫描全天候版) ==========")
    session = get_robust_session()
    
    spot_df = get_hk_market_snapshot(session)
    if spot_df.empty: return[], f"❌ {market}股大盘数据为空"
    
    total = len(spot_df)
    for col in ['trade', 'prev_close', 'mktcap']: 
        spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce')
    
    spot_df['trade'] = spot_df['trade'].fillna(spot_df['prev_close'])
    spot_df.loc[spot_df['trade'] == 0, 'trade'] = spot_df['prev_close']
    
    print(f"🌊 启用港股全景模式：深度扫描全部 {total} 只港股！")
    # 🌟 港股过滤核心法则：剔除仙股、剔除小市值老千股 (股价>=1港元, 市值>=100亿)
    f_df = spot_df[(spot_df['trade'] >= 1.0) & (spot_df['mktcap'] >= 10000000000)].copy()
    
    print(f"💰 基础过滤完成：剩余 {len(f_df)} 只候选百亿级标的！启动并发引擎...")
    
    final_stocks =[]
    fail_reasons = defaultdict(int)

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        futures = {executor.submit(process_single_hk_stock, row, session): row['code'] for _, row in f_df.iterrows()}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res["status"] == "success": final_stocks.append(res["data"])
            elif res["status"] == "fail": fail_reasons[res["reason"]] += 1

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fail_str = "".join([f"   - {r}: {c} 只\n" for r, c in sorted(fail_reasons.items(), key=lambda x:x[1], reverse=True)])
    diag_msg = (
        f"[{now}] 港股(HK)诊断报告：\n"
        f"📊 市场基数: {total}只 | 流动性百亿过滤池: {len(f_df)}只\n"
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
    print(f"[{now}] 💻 用户选择: 港股专属模式！开始执行...")
    
    # 彻底关闭A股执行流，专注跑港股，极大提高执行效率并绕开无用报错！
    try:
        res_hk, msg_hk = run_screener("HK")
        # ⚠️ 请确保输出表里存在名叫 "HK-Share Screener" 的工作表 (Tab)
        write_to_sheet("HK-Share Screener", res_hk, "60D_Return%", diag_msg=msg_hk)
    except Exception as e:
        error_info = traceback.format_exc()
        print(f"港股写入崩溃，错误详情:\n{error_info}")
