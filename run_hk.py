import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, requests, json, re, concurrent.futures, warnings, traceback, random, time
from collections import defaultdict
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

warnings.filterwarnings('ignore')

# ==========================================
# 1. 基础设置与双向 Google Sheets 连接
# ==========================================
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"

scopes =["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

def get_robust_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=30, pool_maxsize=30)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://quote.eastmoney.com/'
    })
    return session

# ==========================================
# 2. 市场大盘数据获取 (港股全市场扫描)
# ==========================================
def get_hk_market_snapshot(session):
    print("🚀 启动【东方财富】抓取 港股(HK) 全市场基础代码库...")
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    all_data =[]
    page = 1
    while True:
        params = {
            "pn": str(page), "pz": "500", "po": "1", "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281", "fltt": "2", "invt": "2",
            "fid": "f3", "fs": "m:116+t:3,m:116+t:4",  # 港股主板核心
            "fields": "f12,f14,f2,f18,f20"
        }
        try:
            res = session.get(url, params=params, timeout=10).json()
            if not res or 'data' not in res or not res['data'] or not res['data'].get('diff'): break
            diff = res['data']['diff']
            all_data.extend(diff)
            if len(diff) < 500: break
            page += 1
        except Exception:
            time.sleep(1)
            
    df = pd.DataFrame(all_data)
    if not df.empty:
        df.rename(columns={'f12': 'code', 'f14': 'name', 'f2': 'trade', 'f18': 'prev_close', 'f20': 'mktcap'}, inplace=True)
    return df

# ==========================================
# 3. K线获取
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
        except Exception: time.sleep(0.2)
    return None

# ==========================================
# 4. 核心选股引擎 (三轨制：经典 + 起爆 + 老龙回头)
# ==========================================
def apply_advanced_logic(code, name, klines, mktcap):
    valid_klines =[k.split(',') for k in klines if len(k.split(',')) >= 8]
    if len(valid_klines) < 250: return {"status": "fail", "reason": "次新/数据不足"}

    k_matrix = np.array(valid_klines)
    opens = k_matrix[:, 1].astype(float)
    closes = k_matrix[:, 2].astype(float)
    highs = k_matrix[:, 3].astype(float)
    lows = k_matrix[:, 4].astype(float)
    vols = k_matrix[:, 5].astype(float)
    amounts = k_matrix[:, 6].astype(float) 
    
    close = closes[-1]
    last_amount = amounts[-1]
    
    if close == 0.0 or vols[-1] == 0: return {"status": "fail", "reason": "停牌/无数据"}
    if last_amount < 30000000: return {"status": "fail", "reason": "成交极度萎靡(<3000万)"} # 稍微放宽以容纳起爆前奏

    # --- 基础技术指标计算 ---
    ma20 = np.mean(closes[-20:])
    ma50 = np.mean(closes[-50:])
    ma60 = np.mean(closes[-60:])
    ma150 = np.mean(closes[-150:])
    ma200 = np.mean(closes[-200:])
    
    h250, l250 = np.max(highs[-250:]), np.min(lows[-250:])
    dist_high_pct = (close - h250) / h250 if h250 > 0 else 0
    
    avg_v50 = np.mean(vols[-50:])
    vol_ratio_today = vols[-1] / avg_v50 if avg_v50 > 0 else 0
    pct_change_today = (close - closes[-2]) / closes[-2] if closes[-2] > 0 else 0
    
    r120_ret = (close - closes[-121]) / closes[-121] if closes[-121] > 0 else 0

    # -----------------------------------------------------
    # 🌟 轨线 1：老龙回头 (黄金坑 + 神奇九转 + 放量点火)
    # -----------------------------------------------------
    # 1. 探底神针：计算近3日内是否出现过“神奇九转(TD9)”底结构买点
    td_buy = np.zeros(len(closes))
    for i in range(4, len(closes)):
        if closes[i] < closes[i-4]: td_buy[i] = td_buy[i-1] + 1
        else: td_buy[i] = 0
    has_td9_bottom = np.any(td_buy[-3:] >= 9)  # 近3天内出现过9转买点
    
    # 替代B信号：昨日收长阴，今日强力反包 (看涨吞没)
    is_bullish_engulfing = (close > opens[-2]) and (closes[-2] < opens[-2]) and (pct_change_today > 0.02)
    bottom_signal = has_td9_bottom or is_bullish_engulfing

    is_old_dragon = (
        r120_ret > 0.12 and                        # 1. 曾经是王者 (120日收益>12% 稍微放宽容纳港股)
        (-0.25 <= dist_high_pct <= -0.05) and      # 2. 完美洗盘 (距离最高点回落 5%~25% 之间)
        (close < ma20) and (close >= ma60 * 0.98) and # 3. 恐慌跌破20日线，但精准踩在60日生命线上！
        bottom_signal and                          # 4. 出现9转结构 或 看涨吞没
        (vol_ratio_today > 1.5) and (pct_change_today > 0.03) # 5. 探明拐点：今日量比>1.5 且 涨幅>3%
    )

    # -----------------------------------------------------
    # 🚀 轨线 2：底部放量起爆 (口袋支点 Pocket Pivot)
    # -----------------------------------------------------
    vol_ratio_3d = np.max(vols[-3:]) / avg_v50 if avg_v50 > 0 else 0
    daily_returns_3d = [(closes[-i] - closes[-i-1])/closes[-i-1] for i in range(1, 4)]
    max_daily_ret_3d = max(daily_returns_3d)

    is_explosive_breakout = (
        vol_ratio_3d >= 2.0 and        # 近3天内有爆量2倍以上建仓
        max_daily_ret_3d >= 0.04 and   # 近3天有单日大阳线(>4%)
        close > ma50 and close > ma20 and # 强力站上短期均线
        close >= h250 * 0.50           # 非深渊死猫跳
    )

    # -----------------------------------------------------
    # 📈 轨线 3：经典欧奈尔多头 (抓主升浪)
    # -----------------------------------------------------
    is_standard_uptrend = (
        close > ma20 and close > ma50 and 
        ma50 > ma150 and ma150 > ma200 and 
        close >= h250 * 0.75  # 距离新高25%以内 (港股波动大，参数比美股放宽)
    )

    # ================= 裁决逻辑 =================
    if not (is_old_dragon or is_explosive_breakout or is_standard_uptrend): 
        return {"status": "fail", "reason": "未触发任何策略体系"}
        
    # 防止追高极限限制 (偏离50日线超过 30% 极为危险)
    if close > (ma50 * 1.30): 
        return {"status": "fail", "reason": "偏离50日线>30%(极度超买)"}

    # 计算 RSI 动量
    deltas = np.diff(closes[-30:])
    up = pd.Series(np.where(deltas > 0, deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
    down = pd.Series(np.where(deltas < 0, -deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
    rsi = 100 - (100 / (1 + (up/down))) if down > 0 else 100
    
    if rsi < 50: return {"status": "fail", "reason": "RSI<50(反转动能未确认)"} 

    # 动态生成专属战法标签
    trend_tag =[]
    if is_old_dragon: trend_tag.append("🐉 老龙回头(黄金坑)")
    if is_explosive_breakout: trend_tag.append("🚀 底部放量起爆")
    if is_standard_uptrend: trend_tag.append("📈 经典多头排列")
    if is_standard_uptrend and is_explosive_breakout: trend_tag = ["🔥 完美共振(多头+起爆)"]

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
        "Dist_High%": f"{round(dist_high_pct * 100, 2)}%",
        "Mkt_Cap(亿)": round(mktcap / 100000000, 2), 
        "Turnover(亿)": round(last_amount / 100000000, 2),
        "Trend": " + ".join(trend_tag)
    }
    return {"status": "success", "data": data}

def process_single_hk_stock(row, session):
    pure_code = str(row['code']).zfill(5)
    name = row['name']
    try:
        klines = fetch_kline_data(f"116.{pure_code}", session)
        if not klines: 
            klines = fetch_kline_data(f"128.{pure_code}", session)
            if not klines: return {"status": "fail", "reason": "节点阻断"}
        return apply_advanced_logic(f"{pure_code}.HK", name, klines, row['mktcap'])
    except Exception: return {"status": "fail", "reason": "解析异常"}

# ==========================================
# 5. 主程序筛选控制
# ==========================================
def run_screener():
    print(f"\n========== 开始处理 港股(HK) (三轨制全天候版) ==========")
    session = get_robust_session()
    
    spot_df = get_hk_market_snapshot(session)
    if spot_df.empty: return[], "❌ 港股大盘数据为空"
    
    for col in ['trade', 'prev_close', 'mktcap']: 
        spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce')
    
    spot_df['trade'] = spot_df['trade'].fillna(spot_df['prev_close'])
    
    # 🌟 核心市值过滤条件：只要百亿级别中军 (过滤小盘老千股)，股价>1港币
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
        f"📊 市场百亿过滤池: {len(f_df)}只\n"
        f"🏆 最终选出最强龙头: {min(len(final_stocks), 50)}只\n"
        f"🔪 淘汰明细：\n{fail_str}"
    )
    return final_stocks, diag_msg

def write_to_sheet(sheet_name, final_stocks, diag_msg=None):
    try:
        sheet = client.open_by_url(OUTPUT_SHEET_URL).worksheet(sheet_name)
        sheet.clear()
        if final_stocks:
            df = pd.DataFrame(final_stocks)
            # 根据 60日收益率 降序排列，强者恒强
            df['Sort_Num'] = df['60D_Return%'].str.replace('%', '').astype(float)
            df = df.sort_values(by='Sort_Num', ascending=False).drop(columns=['Sort_Num'])
            df = df.head(50) 
            sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
            
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.update_acell("M1", "Last Updated:")
            sheet.update_acell("N1", now)
            if diag_msg: sheet.update_acell("O1", diag_msg)
            print(f"🎉 成功将 {sheet_name} 前 {len(df)} 只最强龙头写入表格！")
        else:
            sheet.update_acell("A1", "今日无符合条件的股票。")
            print(f"⚠️ {sheet_name} 已写入空仓报告。")
    except Exception as e: 
        print(f"❌ 写入 {sheet_name} 失败: {e}")

if __name__ == "__main__":
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] 💻 执行港股三轨制选股器(包含老龙回头战略)...")
    
    try:
        res_hk, msg_hk = run_screener()
        write_to_sheet("HK-Share Screener", res_hk, diag_msg=msg_hk)
        print(msg_hk)
    except Exception as e:
        print(f"港股执行崩溃:\n{traceback.format_exc()}")
