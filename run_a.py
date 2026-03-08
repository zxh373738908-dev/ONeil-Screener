import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, requests, json, re, time, concurrent.futures, warnings, traceback
import akshare as ak
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

# 🚀 黑科技：绝对数值清洗器 (将 0.205, "20.5%", "20.5" 统一清洗为 20.5)
def parse_pct(val):
    if pd.isna(val) or str(val).strip() == '': return 0.0
    s = str(val).replace(',', '').strip()
    is_pct = '%' in s
    s = s.replace('%', '')
    try:
        f = float(s)
        # 如果没有 % 且数值在 -2 到 2 之间，极大概率是小数格式 (如 0.205 代表 20.5%)
        if not is_pct and -2 <= f <= 2:
            return f * 100.0
        return f
    except:
        return 0.0

def parse_float(val):
    if pd.isna(val) or str(val).strip() == '': return 0.0
    try: return float(str(val).replace(',', '').strip())
    except: return 0.0

# ==========================================
# 2. [第一阶] 读取板块大方向 (智能模糊匹配版)
# ==========================================
def get_target_sectors():
    print("\n🌍 [STEP 1] 正在连接宏观大盘，读取板块景气度模型...")
    try:
        sheet = client.open_by_url(SECTOR_SHEET_URL).sheet1
        raw_data = sheet.get_all_values()
        
        if len(raw_data) < 2: return []
            
        headers = [str(h).strip() for h in raw_data[0]]
        df = pd.DataFrame(raw_data[1:], columns=headers)
        
        # 🚀 黑科技：模糊寻找表头 (只要包含这些关键字就能识别)
        def get_fuzzy_col(keywords, is_pct=True):
            for kw in keywords:
                for col in df.columns:
                    if kw.lower() in col.lower():
                        return df[col].apply(parse_pct if is_pct else parse_float)
            print(f"⚠️ 警告: 未找到包含 {keywords} 的列，已用 0.0 填充。")
            return pd.Series(0.0, index=df.index)

        # 智能提取列数据
        r120 = get_fuzzy_col(['R120', '120日'], True)
        rank = get_fuzzy_col(['Rank', '排名', '强度'], False)
        rel20 = get_fuzzy_col(['REL20'], True)
        rel60 = get_fuzzy_col(['REL60'], True)
        r60 = get_fuzzy_col(['R60', '60日'], True)
        r20 = get_fuzzy_col(['R20', '20日'], True)
        rel5 = get_fuzzy_col(['REL5', '5日'], True)
        
        # 打印第一行解析后的数据，让你一眼看出系统是不是算错了！
        name_col = df.columns[0]
        for col in df.columns:
            if '名' in col or 'Name' in col: name_col = col
            
        print(f"\n🔍 [上帝视角] 第一行数据清洗结果展示:")
        print(f"板块名称: {df[name_col].iloc[0]}")
        print(f"解析数值 -> R120: {r120.iloc[0]}%, Rank: {rank.iloc[0]}, REL20: {rel20.iloc[0]}%, REL60: {rel60.iloc[0]}%, R60: {r60.iloc[0]}%, R20: {r20.iloc[0]}%, REL5: {rel5.iloc[0]}%")
        print("-" * 40)
        
        # 👑【强势热门板块】 (注意：清洗后 20% 就是 20.0，所以这里用 > 20)
        cond_main = (r120 > 20.0) & (rank >= 80) & (rel20 > 0) & (rel60 > 0) & (r60 > 0)
        
        # 🎯【加速期回踩】
        cond_dip = (r120 > 15.0) & (r20 < 0) & (rel5 > 0)
        
        hot_sectors = df[cond_main][name_col].tolist()
        dip_sectors = df[cond_dip][name_col].tolist()
        
        all_target_sectors = list(set(hot_sectors + dip_sectors))
        print(f"✅ 宏观模型运算完毕：锁定 {len(hot_sectors)} 个主战场，{len(dip_sectors)} 个黄金坑。")
        
        if all_target_sectors:
            print(f"🎯 最终核心攻击板块: {', '.join(all_target_sectors[:10])} ...")
            
        return all_target_sectors
    except Exception as e:
        print(f"⚠️ 读取板块表格发生解析错误: {e}")
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
        
        print(f"✅ 成功提取！共锁定 {len(target_tickers)} 只具备板块效应的个股。")
        return target_tickers
    except Exception as e:
        print(f"⚠️ 提取成分股失败: {e}")
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
        return {"status": "success", "data": data, "log": f"✅ 捕获主升浪: {pure_code} {name}"}
    except Exception as e: 
        err_msg = str(e)[:10]
        return {"status": "fail", "reason": f"接口错({err_msg})"}

# ==========================================
# 6. 主程序流转控制
# ==========================================
def screen_a_shares():
    print("\n========== 开始处理 A股 (严格自上而下 Top-Down) ==========")
    
    target_sectors = get_target_sectors()
    if not target_sectors:
        print("\n🛑 [强制熔断] 宏观模型未发现符合要求的强庄板块。")
        print("💡 欧奈尔纪律：倾巢之下无完卵。强制执行空仓纪律！")
        return [], "[系统保护] 宏观大盘无符合条件的热门板块，严格执行空仓纪律！"
        
    core_tickers = get_stocks_from_sectors(target_sectors)
    if not core_tickers:
        return [], "❌ 提取板块成分股失败，请检查网络。"
    
    print("\n📊 [STEP 3] 扫描全市场流动性...")
    spot_df = get_sina_market_snapshot()
    if spot_df.empty: return [], "❌ 大盘数据为空"
    
    for col in ['trade','mktcap','amount','turnoverratio']: spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce')
    spot_df['mktcap'] *= 10000
    
    spot_df['pure_code'] = spot_df['code'].apply(lambda x: str(x)[-6:])
    f_df = spot_df[spot_df['pure_code'].isin(core_tickers)].copy()
    print(f"🎯 板块降维完成：全市场 5000 只股票 -> 目标池骤降至 {len(f_df)} 只板块核心股。")
        
    f_df = f_df[(f_df['trade']>=10) & (f_df['mktcap']>=5000000000) & (f_df['amount']>=200000000) & (f_df['turnoverratio']>=1.5)].copy()
    print(f"💰 流动性过滤完成：即将对剩余 {len(f_df)} 只硬核标的进行 K 线狙击！")
    
    if f_df.empty:
        return [], "[系统保护] 主线板块内的个股流动性严重枯竭，建议空仓！"
    
    final_stocks = []
    fail_reasons = defaultdict(int)
    
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
            print(f"🎉 成功将 {len(df)} 只标的写入表格！")
        else:
            sheet.update_acell("A1", diag_msg if diag_msg else "无符合条件的股票。")
            print("⚠️ 筛选结束，已写入空仓诊断报告。")
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
