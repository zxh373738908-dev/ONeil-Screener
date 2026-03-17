import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime
import warnings
import traceback
import yfinance as yf
import logging
import requests
import re
from collections import defaultdict

warnings.filterwarnings('ignore')
# 屏蔽 yfinance 内部烦人的警告输出
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. 基础设置与 Google Sheets 连接
# ==========================================
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# 强制绑定 UTC+8 北京/香港时间
TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

def get_worksheet(sheet_name="HK-Share Screener"):
    doc = client.open_by_url(OUTPUT_SHEET_URL)
    try:
        return doc.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        return doc.add_worksheet(title=sheet_name, rows=100, cols=20)

# ==========================================
# 🌍 STEP 1: 获取港股名册 (TradingView 国际接口无视封锁版)
# ==========================================
def get_hk_share_list():
    print("\n🌍 [STEP 1] 启动【底层脱壳机制】：侦测到国内源联合封锁，切换至国际级【TradingView 量化中枢】...")
    
    url = "https://scanner.tradingview.com/hongkong/scan"
    
    payload = {
        "columns": ["name", "description", "close", "market_cap_basic"],
        "range": [0, 4000],  # 一次性拉取全部港股
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "filter": [
            # 仅筛选纯正股票，排除权证、牛熊证等杂项
            {"left": "type", "operation": "equal", "right": "stock"}
        ]
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Origin": "https://www.tradingview.com",
        "Referer": "https://www.tradingview.com/"
    }
    
    try:
        print("   -> 🔄 正在跨域连接 TradingView 主节点建立通讯...")
        resp = requests.post(url, json=payload, headers=headers, timeout=20)
        
        if resp.status_code != 200:
            raise ValueError(f"HTTP状态异常: {resp.status_code}")
            
        data = resp.json()
        raw_list = data.get("data", [])
        
        if not raw_list:
            raise ValueError("获取到的股票列表为空")
            
        stock_list = []
        for item in raw_list:
            fields = item.get("d", [])
            if len(fields) >= 4:
                raw_code, name, price, mktcap = fields[0], fields[1], fields[2], fields[3]
                clean_sym = re.sub(r'[^0-9]', '', str(raw_code))
                if not clean_sym: continue
                
                stock_list.append({
                    "代码": clean_sym,
                    "名称": name,
                    "最新价": float(price) if price is not None else 0.0,
                    "总市值": float(mktcap) if mktcap is not None else 0.0
                })
                
        df = pd.DataFrame(stock_list)
        print(f"   -> ✅ TradingView 国际专线接入成功！提取全市场 {len(df)} 只基础名册。")
        
    except Exception as e:
        print(f"   -> ❌ 致命错误：TradingView 数据流被阻断: {e}")
        return pd.DataFrame()

    # 🌟 核心过滤条件：股价 >= 1港币 且 市值 >= 100亿港币
    df = df[(df['最新价'] >= 1.0) & (df['总市值'] >= 10000000000)].copy()
    df = df[df['名称'].astype(str).str.strip() != '']
    
    print(f"   -> ✅ 基础洗盘完毕！通过真实市值提纯出 {len(df)} 只【百亿级】候选标的，送往 Yahoo 天基演算。")
    return df[['代码', '名称', '最新价', '总市值']]

# ==========================================
# 🧠 STEP 2: 核心选股引擎 (三轨制：经典 + 起爆 + 老龙回头)
# ==========================================
def apply_advanced_logic(ticker, name, opens, closes, highs, lows, vols, amounts, mktcap):
    # 1. 基础数据防爆校验：确保有足够的K线数据计算250日新高和长线均线
    if len(closes) < 250: 
        return {"status": "fail", "reason": "次新/数据不足250天"}

    close = closes[-1]
    last_amount = amounts[-1]
    
    # 取近5日平均成交额，防单日异常骗线 (港股极其重要)
    avg_amount_5d = np.mean(amounts[-5:]) 
    
    if close == 0.0 or vols[-1] == 0: 
        return {"status": "fail", "reason": "停牌/今日无数据"}
        
    # [优化] 流动性护城河提升至 5000 万，过滤掉容易被操纵的仙股/庄股
    if avg_amount_5d < 50000000: 
        return {"status": "fail", "reason": "5日均成交萎靡(<5000万)"} 

    # --- 基础技术指标计算 ---
    ma20 = np.mean(closes[-20:])
    ma50 = np.mean(closes[-50:])
    ma60 = np.mean(closes[-60:])
    ma150 = np.mean(closes[-150:])
    ma200 = np.mean(closes[-200:])

    h250 = np.max(highs[-250:])
    dist_high_pct = (close - h250) / h250 if h250 > 0 else 0

    avg_v50 = np.mean(vols[-50:])
    vol_ratio_today = vols[-1] / avg_v50 if avg_v50 > 0 else 0
    pct_change_today = (close - closes[-2]) / closes[-2] if closes[-2] > 0 else 0

    r120_ret = (close - closes[-121]) / closes[-121] if closes[-121] > 0 else 0

    # 计算 RSI (14日) - 用于动量过滤
    deltas = np.diff(closes[-15:])
    up = pd.Series(np.where(deltas > 0, deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
    down = pd.Series(np.where(deltas < 0, -deltas, 0)).ewm(com=13, adjust=False).mean().iloc[-1]
    rsi = 100 - (100 / (1 + (up/down))) if down > 0 else 100

    # -----------------------------------------------------
    # 🌟 轨线 1：老龙回头 (核心优化：长线多头 + 缩量回踩关键均线 + 收阳企稳)
    # -----------------------------------------------------
    touch_ma20 = abs(close - ma20) / ma20 <= 0.03
    touch_ma50 = abs(close - ma50) / ma50 <= 0.03
    
    is_old_dragon = (
        (ma50 > ma150) and                  # 大级别趋势必须是向上的
        (touch_ma20 or touch_ma50) and      # 刚好回踩到支撑线附近
        (close > opens[-1]) and             # 今天必须是红盘阳线（拒绝接飞刀）
        (vol_ratio_today < 1.2) and         # 回踩时量能不能太大（证明主力没出货，是洗盘）
        (rsi > 40)                          # RSI没有进入极度弱势区
    )

    # -----------------------------------------------------
    # 🚀 轨线 2：底部/平台放量起爆 (核心优化：放量倍数 + 突破MA20 + 涨幅要求)
    # -----------------------------------------------------
    is_explosive_breakout = (
        (vol_ratio_today >= 1.8) and        # 爆量：今日成交量是过去50天均量的1.8倍以上
        (pct_change_today >= 0.035) and     # 实体大阳线：日涨幅 > 3.5%
        (close > ma20) and                  # 强势突破20日生命线
        (close >= h250 * 0.60)              # 拒绝底部挣扎的垃圾股，至少在年内半山腰以上
    )

    # -----------------------------------------------------
    # 📈 轨线 3：经典欧奈尔多头 (核心优化：严苛的均线排列 + 逼近新高)
    # -----------------------------------------------------
    is_standard_uptrend = (
        (close > ma20) and (ma20 > ma50) and (ma50 > ma150) and (ma150 > ma200) and # 完美多头排列
        (close >= h250 * 0.80) and          # 距离一年最高点不到20% (上方几乎无套牢盘)
        (rsi > 55)                          # 动量强劲
    )

    # ================= 裁决逻辑 =================
    if not (is_old_dragon or is_explosive_breakout or is_standard_uptrend): 
        return {"status": "fail", "reason": "未触发极品作战形态"}
        
    if close > (ma50 * 1.25): 
        return {"status": "fail", "reason": "偏离50日线>25%(极度超买，盈亏比差)"}

    if close < ma200 and not is_explosive_breakout:
        return {"status": "fail", "reason": "跌破200日牛熊线"}

    # 🏷️ 动态生成专属战法标签
    trend_tag = []
    if is_old_dragon: trend_tag.append("🐉老龙回头(踩均线)")
    if is_explosive_breakout: trend_tag.append("🚀放量起爆(主力点火)")
    if is_standard_uptrend: trend_tag.append("📈经典多头(逼近新高)")
    
    if is_standard_uptrend and is_explosive_breakout: 
        trend_tag = ["🔥主升浪爆发(多头+起爆)"]

    close_60 = closes[-61]
    ret_60 = (close - close_60) / close_60 if close_60 > 0 else 0

    # ================= 组装战报 =================
    # 【核心修复】：全部输出纯数字格式，不再使用"%"字符串，彻底解决降序排序报错问题
    data = {
        "Ticker": ticker.replace(".HK", ""), 
        "Name": name, 
        "Price": round(close, 2), 
        "60D_Return": round(ret_60 * 100, 2),        # 纯数字，代表百分比
        "RSI": round(rsi, 2), 
        "Vol_Ratio": round(vol_ratio_today, 2),
        "Dist_High(%)": round(dist_high_pct * 100, 2), # 纯数字，负数代表距离最高点的跌幅
        "Mkt_Cap(亿)": round(mktcap / 100000000, 2), 
        "Turnover(亿)": round(last_amount / 100000000, 2),
        "Trend": " + ".join(trend_tag)
    }
    return {"status": "success", "data": data}

# ==========================================
# 🚀 STEP 3: Yahoo Finance 并发盲扫与分发
# ==========================================
def scan_hk_market_via_yfinance(df_list):
    print("\n🚀 [STEP 2] 启动【Yahoo Finance】天基武器，执行高速大盘演算...")
    
    tickers = []
    ticker_to_info = {}
    
    for _, row in df_list.iterrows():
        code = str(row['代码'])
        yf_code = code.lstrip('0').zfill(4) + '.HK'
        tickers.append(yf_code)
        ticker_to_info[yf_code] = {
            'name': row['名称'],
            'mktcap': row['总市值']
        }
        
    print(f"   -> 📡 构建完成 {len(tickers)} 条数据通道，开始高维批量下载 (请求周期: 2年)...")
    if not tickers:
        print("   -> ❌ 致命错误：转换后的 Yahoo Tickers 列表为空！")
        return [], {}
    
    all_results = []
    fail_reasons = defaultdict(int)
    chunk_size = 500  
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"   -> 📥 正在下载演算第 {i+1} ~ {min(i+chunk_size, len(tickers))} 只核心标的...")
        
        data = yf.download(chunk, period="2y", auto_adjust=True, threads=True, progress=False)
        
        for ticker in chunk:
            try:
                if len(chunk) > 1:
                    closes = data['Close'][ticker].dropna().values
                    opens = data['Open'][ticker].dropna().values
                    highs = data['High'][ticker].dropna().values
                    lows = data['Low'][ticker].dropna().values
                    vols = data['Volume'][ticker].dropna().values
                else:
                    closes = data['Close'].dropna().values
                    opens = data['Open'].dropna().values
                    highs = data['High'].dropna().values
                    lows = data['Low'].dropna().values
                    vols = data['Volume'].dropna().values
                    
                if len(closes) < 250:
                    fail_reasons["次新/数据不足(<250天)"] += 1
                    continue
                
                amounts = closes * vols 
                
                info = ticker_to_info[ticker]
                res = apply_advanced_logic(ticker, info['name'], opens, closes, highs, lows, vols, amounts, info['mktcap'])
                
                if res["status"] == "success":
                    all_results.append(res["data"])
                else:
                    fail_reasons[res["reason"]] += 1
                    
            except KeyError:
                fail_reasons["接口丢包/退市"] += 1
                continue
            except Exception:
                fail_reasons["数据异常截断"] += 1
                continue
                
    return all_results, fail_reasons

# ==========================================
# 📝 STEP 4: 写入作战指令
# ==========================================
def write_sheet(final_stocks, diag_msg=None):
    print("\n📝 [STEP 3] 正在将绝密作战名单写入 Google Sheets 表格...")
    sheet_name = "HK-Share Screener"
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")

    try:
        sheet = get_worksheet(sheet_name)
        sheet.clear()

        if len(final_stocks) == 0:
            sheet.update_acell("A1", "No Signal: 战局恶劣或未发现极品标的。")
            if diag_msg: sheet.update_acell("A3", diag_msg)
            print(f"⚠️ {sheet_name} 已写入空仓报告。")
            return

        df = pd.DataFrame(final_stocks)
        
        # 【核心修复】：直接根据 float 类型的 '60D_Return' 降序排列，去掉了所有多余的文本替换处理
        df = df.sort_values(by='60D_Return', ascending=False)
        df = df.head(50) # 仅向指挥部汇报最强前 50 只龙头

        # 写入表头及数据
        sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
        
        # 写入更新时间戳与诊断日志
        sheet.update_acell("L1", "Last Updated(UTC+8):")
        sheet.update_acell("M1", now_str)
        if diag_msg: 
            sheet.update_acell("N1", diag_msg)
            
        print(f"🎉 大功告成！已成功将 {len(df)} 只战法认证龙头送达指挥部！")
    except Exception as e:
        print(f"❌ 表格写入失败: {e}")

# ==========================================
# MAIN 主函数
# ==========================================
def main():
    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n========== 港股猎手系统 V10.0 (实战极品起爆版) ==========")
    print(f"⏰ 当前系统时间 (UTC+8): {now_str}")
    
    # 1. 获取名单
    df_list = get_hk_share_list()
    if df_list.empty: 
        return
    
    # 2. 批量扫描核心逻辑
    final_stocks, fail_reasons = scan_hk_market_via_yfinance(df_list)
    
    # 3. 构建诊断报告
    fail_str = "".join([f"   - {r}: {c} 只\n" for r, c in sorted(fail_reasons.items(), key=lambda x:x[1], reverse=True)])
    diag_msg = (
        f"[{now_str}] 港股战法扫描完毕：\n"
        f"📊 百亿基础过滤池: {len(df_list)}只\n"
        f"🏆 筛选出极品形态: {len(final_stocks)}只\n"
        f"🔪 淘汰明细：\n{fail_str}"
    )
    print("\n" + diag_msg)
    
    # 4. 写入网盘
    write_sheet(final_stocks, diag_msg=diag_msg)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ 系统发生致命异常:\n{traceback.format_exc()}")
