import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, requests, json, re, time, concurrent.futures, warnings, traceback
from collections import defaultdict
import akshare as ak
warnings.filterwarnings('ignore')

# ==========================================
# 1. 基础设置与双向 Google Sheets 连接
# ==========================================
# 写入选股结果的表格
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"
# 读取宏观板块方向的表格
SECTOR_SHEET_URL = "https://docs.google.com/spreadsheets/d/1BoYIVL3lb8nZE3U1qAkuO3MTrM117x2qycN1RdrDZgo/edit?gid=0#gid=0"

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# 辅助函数：安全转换百分比字符串为浮点数 (如 "20.5%" -> 0.205)
def safe_pct(val):
    if isinstance(val, (int, float)): return float(val)
    if isinstance(val, str):
        val = val.replace('%', '').strip()
        try: return float(val) / 100.0
        except: return 0.0
    return 0.0

def safe_float(val, default=0.0):
    try: return float(val)
    except: return default

# ==========================================
# 2. [第一阶] 读取板块大方向 (Top-Down 核心)
# ==========================================
def get_target_sectors():
    print("\n🌍 [STEP 1] 正在连接宏观大盘，读取板块景气度模型...")
    try:
        sheet = client.open_by_url(SECTOR_SHEET_URL).sheet1
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        # 统一解析所有需要的指标字段
        df['R120_val'] = df['R120'].apply(safe_pct)
        df['R60_val'] = df['R60'].apply(safe_pct)
        df['R20_val'] = df['R20'].apply(safe_pct)
        df['Rank_val'] = df['Rank'].apply(safe_float)
        df['REL120_val'] = df['REL120'].apply(safe_pct) if 'REL120' in df.columns else 0.0
        df['REL60_val'] = df['REL60'].apply(safe_pct)
        df['REL20_val'] = df['REL20'].apply(safe_pct)
        df['REL5_val'] = df['REL5'].apply(safe_pct)
        
        # 👑【列表一：强势热门板块 (主战场)】
        # R120 > 20% 且 Rank >= 80 且 REL20 > 0 且 REL60 > 0 且 R60 > 0
        cond_main = (df['R120_val'] > 0.20) & (df['Rank_val'] >= 80) & \
                    (df['REL20_val'] > 0) & (df['REL60_val'] > 0) & (df['R60_val'] > 0)
        
        # 🎯【列表二：加速期回踩 (真正的“黄金坑”)】
        # R120 > 15% 且 R20 < 0 且 REL5 > 0
        cond_dip = (df['R120_val'] > 0.15) & (df['R20_val'] < 0) & (df['REL5_val'] > 0)
        
        hot_sectors = df[cond_main]['名称'].tolist() if '名称' in df.columns else df.iloc[:, 0][cond_main].tolist()
        dip_sectors = df[cond_dip]['名称'].tolist() if '名称' in df.columns else df.iloc[:, 0][cond_dip].tolist()
        
        all_target_sectors = list(set(hot_sectors + dip_sectors))
        print(f"✅ 宏观降维成功！锁定 {len(hot_sectors)} 个主战场板块，{len(dip_sectors)} 个黄金坑板块。")
        print(f"🎯 核心攻击方向: {', '.join(all_target_sectors[:10])} ...")
        
        return all_target_sectors
    except Exception as e:
        print(f"⚠️ 读取板块表格失败，退回全市场扫描模式: {e}")
        return []

# ==========================================
# 3. [第二阶] 锁定板块成分股
# ==========================================
def get_stocks_from_sectors(sector_names):
    if not sector_names: return set()
    print("\n🧬 [STEP 2] 正在提取核心板块的所有成分股代码...")
    target_tickers = set()
    try:
        em_boards = ak.stock_board_industry_name_em()
        valid_board_names = em_boards['板块名称'].tolist()
        
        for name in sector_names:
            # 清洗名字匹配东财板块 (去ETF/指数等字眼)
            clean_name = name.replace('ETF', '').replace('指数', '').replace('行业', '')
            matched_name = None
            for b_name in valid_board_names:
                if clean_name in b_name or b_name in clean_name:
                    matched_name = b_name
                    break
            
            if matched_name:
                try:
                    cons = ak.stock_board_industry_cons_em(symbol=matched_name)
                    tickers = cons['代码'].tolist()
                    target_tickers.update(tickers)
                except: continue
        
        print(f"✅ 成功提取核心股票池！共锁定 {len(target_tickers)} 只具备板块效应的个股。")
        return target_tickers
    except Exception as e:
        print(f"⚠️ 提取成分股失败: {e}")
        return set()

# ==========================================
# 4. [第三阶] 新浪基础数据 (仅用于流动性过滤)
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
# 5. [第四阶] 腾讯极速 K 线引擎 (防屏蔽核武器)
# ==========================================
def process_single_stock(row, session):
    pure_code = str(row['code'])[-6:] 
    name = row['name']
    prefix = "sh" if pure_code.startswith(('6', '5')) else "sz"
    
    try:
        # 使用腾讯财经日K线接口，全球CDN加速，永不被墙
        k_url = f"https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newiqkline/get?param={prefix}{pure_code},day,,,300,qfq"
        res = session.get(k_url, timeout=5).json()
        
        if res.get('code') != 0: return {"status": "fail", "reason": "接口无数据"}
            
        data_node = res['data'][f'{prefix}{pure_code}']
        klines = data_node.get('qfqday', data_node.get('day', []))
        
        if len(klines) < 250: return {"status": "fail", "reason": "次新/退市"}
        
        # 腾讯格式: [date, open, close, high, low, vol]
        closes = [float(k[2]) for k in klines]
        highs = [float(k[3]) for k in klines]
        lows = [float(k[4]) for k in klines]
        vols = [float(k[5]) for k in klines]
        
        cs, hs, ls, vs = pd.Series(closes), pd.Series(highs), pd.Series(lows), pd.Series(vols)
        close, close_60 = cs.iloc[-1], cs.iloc[-61]
        if close == 0.0: return {"status": "fail", "reason": "停牌"}
        
        # 还原你的长线多头判断逻辑
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
        return {"status": "success", "data": data, "log": f"✅ 捕获主升浪: {pure_code} {name}"}
    except Exception as e: return {"status": "fail", "reason": "数据解析异常"}

# ==========================================
# 6. 主程序流转控制
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股 (降维打击 + 腾讯极速版) ==========")
    
    # 1. 自上而下获取核心股票池
    target_sectors = get_target_sectors()
    core_tickers = get_stocks_from_sectors(target_sectors)
    
    # 2. 获取大盘快照并进行宏观+微观双重过滤
    print("\n📊 [STEP 3] 扫描全市场流动性...")
    spot_df = get_sina_market_snapshot()
    if spot_df.empty: return[], "❌ 大盘数据为空"
    
    total = len(spot_df)
    for col in ['trade','mktcap','amount','turnoverratio']: spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce')
    spot_df['mktcap'] *= 10000
    
    # 核心过滤：如果你有板块代码，只在核心板块里找！如果没有，扫全市场
    if core_tickers:
        spot_df['pure_code'] = spot_df['code'].apply(lambda x: str(x)[-6:])
        f_df = spot_df[spot_df['pure_code'].isin(core_tickers)].copy()
        print(f"🎯 板块降维完成：目标池缩小至 {len(f_df)} 只板块核心股。")
    else:
        f_df = spot_df.copy()
        
    # 流动性过滤
    f_df = f_df[(f_df['trade']>=10) & (f_df['mktcap']>=5000000000) & (f_df['amount']>=200000000) & (f_df['turnoverratio']>=1.5)].copy()
    print(f"💰 流动性过滤完成：即将对最后剩余的 {len(f_df)} 只硬核标的进行 K 线狙击！")
    
    final_stocks = []
    fail_reasons = defaultdict(int)
    
    # 3. 开启腾讯多线程 K 线测算 (10 线程并发绝对安全)
    print("\n⚔️ [STEP 4] 启动腾讯并发引擎，进行形态测算...")
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_single_stock, row, session): row['code'] for _, row in f_df.iterrows()}
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
            sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
            
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("M1", "Last Updated:")
            sheet.update_acell("N1", now)
            if diag_msg: sheet.update_acell("O1", diag_msg)
        else:
            sheet.update_acell("A1", diag_msg if diag_msg else "无符合条件的股票。")
    except Exception as e: print(f"❌ 写入失败: {e}")

# ==========================================
# 7. 主程序启动
# ==========================================
if __name__ == "__main__":
    try:
        res, msg = screen_a_shares()
        write_to_sheet("A-Share Screener", res, "60D_Return%", diag_msg=msg)
    except Exception as e:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_info = traceback.format_exc()
        write_to_sheet("A-Share Screener", [], "60D_Return%", diag_msg=f"[{now}] 致命崩溃:\n{error_info}")
