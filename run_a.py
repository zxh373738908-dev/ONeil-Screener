import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests
import yfinance as yf

# 屏蔽干扰输出
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 基础配置 (API & Google Sheets)
# ==========================================
MINIMAX_API_KEY = "sk-api-TdnkGqdJs7PgXIw9sxhcsvcKYnX5SeIO4p9d2qvB3-QjHKtJ471Wbij0cSy9A4eCallhwtZebkN8jx8YmSJhd3PP-aMt4mN1eLMv2yDQAWfkKCwtKxDMyYk"
MINIMAX_URL = "https://api.minimax.chat/v1/text/chatcompletion_v2"

OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def get_worksheet():
    try:
        creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
        client = gspread.authorize(creds)
        doc = client.open_by_url(OUTPUT_SHEET_URL)
        try:
            return doc.worksheet("A-Share Screener")
        except gspread.exceptions.WorksheetNotFound:
            return doc.add_worksheet(title="A-Share Screener", rows=1000, cols=20)
    except Exception as e:
        print(f"❌ Google Sheets 连接失败: {e}")
        return None

# ==========================================
# 🤖 MiniMax AI 分析分析模块
# ==========================================
def get_ai_catalyst(stock_code, stock_name):
    """调用 MiniMax 获取精选标的的利好逻辑解读"""
    print(f"   -> 🤖 AI 正在深度透视: {stock_name}({stock_code})...")
    
    headers = {
        "Authorization": f"Bearer {MINIMAX_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # 构建针对 A 股交易者的 Prompt
    prompt = f"""
    作为一名资深的A股量化交易员，请分析股票 {stock_name} (代码: {stock_code}) 最近的利好逻辑。
    当前该股呈现“筹码真空、动量极强、大资金持续流入”的特征。
    请简要回答：
    1. 该股所属的核心热门题材。
    2. 最近1-3天是否有关键公告、行业政策或传闻刺激。
    3. 资金抢筹的逻辑支撑。
    字数限制在 60 字以内，不要废话。
    """
    
    payload = {
        "model": "abab6.5s-chat", # 选用高性价比、响应快的模型
        "messages": [
            {"role": "system", "content": "你是一个专业的A股研究员，擅长从复杂新闻中提炼股价驱动力。"},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1 # 降低随机性，确保分析准确
    }

    try:
        response = requests.post(MINIMAX_URL, headers=headers, json=payload, timeout=15)
        if response.status_code == 200:
            return response.json()['choices'][0]['message']['content'].strip()
        else:
            return f"AI 接口返回异常 ({response.status_code})"
    except Exception as e:
        return f"AI 诊断超时: {str(e)[:20]}"

# ==========================================
# 🛡️ 筹码分布计算核心
# ==========================================
def get_chip_data(df_ticker, lookback=120):
    try:
        hist = df_ticker.tail(lookback)
        p_min, p_max = hist['Low'].min(), hist['High'].max()
        bins = 40
        price_range = np.linspace(p_min, p_max, bins + 1)
        v_dist = np.zeros(bins)
        for _, row in hist.iterrows():
            idx = np.where((price_range[:-1] >= row['Low']) & (price_range[1:] <= row['High']))[0]
            if len(idx) > 0: v_dist[idx] += row['Volume'] / len(idx)
            else:
                c_idx = np.searchsorted(price_range, row['Close']) - 1
                if 0 <= c_idx < bins: v_dist[c_idx] += row['Volume']
        poc_price = (price_range[np.argmax(v_dist)] + price_range[np.argmax(v_dist)+1]) / 2
        curr_price = df_ticker['Close'].iloc[-1]
        curr_idx = np.searchsorted(price_range, curr_price) - 1
        overhead_vol = np.sum(v_dist[curr_idx:]) if curr_idx < bins else 0
        total_vol = np.sum(v_dist)
        res_ratio = (overhead_vol / total_vol) * 100 if total_vol > 0 else 0
        return round(poc_price, 2), f"{round(res_ratio, 1)}%"
    except:
        return 0, "N/A"

# ==========================================
# 🌍 STEP 1: 获取 A 股名册 (暴力穿透版)
# ==========================================
def get_a_share_list():
    print("\n🌍 [STEP 1] 启动【暴力号段生成器】：跳过 API，直接生成 A 股号段...")
    ranges = [
        (600000, 602000), (603000, 606000), # 沪市主板
        (1, 1400), (2000, 3200),           # 深市主板
        (300000, 301650),                  # 创业板
        (688000, 688990)                   # 科创板
    ]
    codes = [f"{i:06d}" for start, end in ranges for i in range(start, end)]
    return pd.DataFrame(codes, columns=['code'])

# ==========================================
# 🚀 STEP 2: T.U.A.W. 战法扫描仪
# ==========================================
def scan_market_via_yfinance(df_list):
    print("\n🚀[STEP 2] 启动【T.U.A.W】战法扫描仪 + 筹码确认...")
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df_list['code']]
    
    all_results = []
    chunk_size = 500
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"   -> 📥 扫描进度: {i}/{len(tickers)} (全球数据中心通道)...")
        try:
            data = yf.download(chunk, period="1y", auto_adjust=True, threads=True, progress=False)
            if data.empty or 'Close' not in data: continue
            
            valid_tickers = data['Close'].columns.tolist() if isinstance(data.columns, pd.MultiIndex) else ([chunk[0]] if not data.empty else [])

            for t in valid_tickers:
                try:
                    df_t = (pd.DataFrame({
                        'Open': data['Open'][t], 'High': data['High'][t],
                        'Low': data['Low'][t], 'Close': data['Close'][t],
                        'Volume': data['Volume'][t]
                    }).dropna()) if isinstance(data.columns, pd.MultiIndex) else data.dropna()
                    
                    if len(df_t) < 200: continue
                    
                    closes, highs, lows, vols = df_t['Close'].values, df_t['High'].values, df_t['Low'].values, df_t['Volume'].values
                    price = closes[-1]
                    
                    # 您的原装逻辑门槛
                    turnover_1 = price * vols[-1]
                    if turnover_1 < 150_000_000 or price < 5: continue 
                    
                    vol_ratio = vols[-1] / np.mean(vols[-50:])
                    h250 = np.max(highs[-250:])
                    r20, r60, r120 = (price/closes[-21]-1), (price/closes[-61]-1), (price/closes[-121]-1)
                    rs_score = (r20*0.4 + r60*0.3 + r120*0.3) * 100
                    dist_high_pct = ((price - h250) / h250) * 100
                    amp5 = np.mean((highs[-5:] - lows[-5:]) / lows[-5:] * 100)

                    # 战法判定 (逻辑 0 修改)
                    cond_mom = (rs_score > 85) or (r60 * 100 > 30)
                    fuse = (-8 <= dist_high_pct <= -1) and (vol_ratio < 1.0) and (amp5 < 5.0) and cond_mom
                    sniper = (-8 <= dist_high_pct <= 2) and (vol_ratio > 1.5) and cond_mom and (price > np.mean(closes[-20:]))
                    
                    if not (fuse or sniper): continue
                    
                    poc, res = get_chip_data(df_t)
                    all_results.append({
                        "Ticker": t.split('.')[0],
                        "Name": "Fetching...",
                        "Price": round(price, 2),
                        "Type": "🔥 狙击" if sniper else "🧨 伏击",
                        "RS_Score": round(rs_score, 2),
                        "POC": poc,
                        "上方抛压": res,
                        "量比": round(vol_ratio, 2),
                        "距高点%": f"{round(dist_high_pct, 2)}%",
                        "AI利好透视": "Waiting..." 
                    })
                except: continue
        except: continue
                
    return all_results

# ==========================================
# 🛰️ STEP 3: 名称修复 + AI 逻辑分析整合
# ==========================================
def repair_and_ai_analyze(results):
    """先修名字，再让 AI 针对名字做解读"""
    if not results: return results
    
    # 按照 RS_Score 排序，只分析前 15 名最强的标的，节省时间并保证质量
    results = sorted(results, key=lambda x: x['RS_Score'], reverse=True)
    top_results = results[:15]
    
    print(f"\n🛰️ [STEP 3] 启动【AI分析 + 名称修复】模块 (目标: 前 {len(top_results)} 只精锐)...")
    
    for item in top_results:
        try:
            # 1. 修复中文名
            t_obj = yf.Ticker(f"{item['Ticker']}.SS" if item['Ticker'].startswith('6') else f"{item['Ticker']}.SZ")
            name = t_obj.info.get('shortName', item['Ticker'])
            item['Name'] = name
            
            # 2. MiniMax AI 利好透视
            item['AI利好透视'] = get_ai_catalyst(item['Ticker'], name)
            time.sleep(1) # 适当延迟，防止触发 AI 频率限制
        except Exception:
            item['Name'] = item['Ticker']
            item['AI利好透视'] = "分析失败"
            
    return top_results

# ==========================================
# 📝 STEP 4: 写入作战名单
# ==========================================
def write_sheet(data):
    sheet = get_worksheet()
    if not sheet: return
    sheet.clear()
    if not data:
        sheet.update_acell("A1", "No Signals Today.")
        return
    
    df = pd.DataFrame(data)
    # 重排各列顺序，让最重要的信息靠前
    cols = ["Ticker", "Name", "Type", "Price", "AI利好透视", "RS_Score", "POC", "上方抛压", "量比", "距高点%"]
    df = df[cols]
    
    sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
    
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    sheet.update_acell("L1", f"V9.1 Last Update (BJ): {now}")
    print(f"🎉 V9.1 指挥系统任务完成！AI 战报已送达。")

# ==========================================
# 主流程
# ==========================================
if __name__ == "__main__":
    print(f"\n{'='*50}\n   A股猎手 V9.1 - 天基智控版 (MiniMax 集成)\n{'='*50}")
    seeds = get_a_share_list()
    raw_hits = scan_market_via_yfinance(seeds)
    final_hits = repair_and_ai_analyze(raw_hits)
    write_sheet(final_hits)
