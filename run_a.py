import akshare as ak
import pandas as pd
import numpy as np
import datetime
import concurrent.futures
import gspread
from google.oauth2.service_account import Credentials
import time
import random
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# Google Sheet 配置
# ==========================================
# ⚠️ 注意：请确保此处是您真实的 Google Sheet URL
OUTPUT_SHEET_URL = "https://docs.google.com/spreadsheets/d/14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8/edit?gid=0#gid=0"

scopes =[
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# 🛡️ 智能获取工作表（如果没有，自动帮您创建！）
def get_worksheet():
    doc = client.open_by_url(OUTPUT_SHEET_URL)
    try:
        return doc.worksheet("A-Share Screener")
    except gspread.exceptions.WorksheetNotFound:
        print("   -> ⚠️ 未找到 'A-Share Screener' 工作表，系统正在自动创建...")
        return doc.add_worksheet(title="A-Share Screener", rows=100, cols=20)

# ==========================================
# STEP1: 东方财富主线雷达 (完美适配 Github 环境)
# ==========================================
def get_hot_tickers():
    print("\n🔥 STEP1 启动【东方财富】主线雷达，扫描今日最强资金风口...")
    try:
        # 获取东方财富行业板块实时数据 (比同花顺稳定 100 倍，无需 JS 验证)
        ind_df = ak.stock_board_industry_name_em()
        
        # 按涨跌幅排序，挑选最猛的前 5 大板块
        top_inds = ind_df.sort_values(by="涨跌幅", ascending=False).head(5)
        hot_sectors = top_inds["板块名称"].tolist()
        print(f"   -> ✅ 锁定当前市场前 5 大强势行业：{hot_sectors}")
        
        hot_tickers = set()
        for sector in hot_sectors:
            try:
                # 获取该板块下的所有成分股
                cons_df = ak.stock_board_industry_cons_em(symbol=sector)
                codes =[str(x).zfill(6) for x in cons_df["代码"].tolist()]
                hot_tickers.update(codes)
                time.sleep(0.3)  # 礼貌停顿，防封锁
            except Exception as e:
                print(f"   -> ⚠️ 板块 [{sector}] 成分股获取波动: {e}")
                
        print(f"   -> 🎯 主线雷达扫描完毕，共提取 {len(hot_tickers)} 只主线热门标的！")
        return list(hot_tickers)
    except Exception as e:
        print(f"   -> ❌ 主线雷达受阻 ({e})，系统将自动降级为全市场盲扫模式。")
        return[]

# ==========================================
# STEP2 市场快照与基底过滤
# ==========================================
def get_market_snapshot(hot_tickers):
    print("\n🚀 STEP2 抓取全市场快照，并进行兵力筛选...")
    for attempt in range(3):
        try:
            df = ak.stock_zh_a_spot_em()
            df = df.rename(columns={
                "代码": "code",
                "名称": "name",
                "最新价": "price",
                "总市值": "mktcap"
            })
            df = df[["code", "name", "price", "mktcap"]]
            
            # 基础过滤：过滤低价仙股和 40亿以下极小盘
            df = df[df["price"] > 5]
            df = df[df["mktcap"] > 4000000000]
            
            # 【双擎护盾】：如果主线获取成功，剔除非主线股票，大幅减少被封 IP 概率！
            if hot_tickers:
                df = df[df["code"].isin(hot_tickers)]
                print(f"   -> ⚔️ 剔除非主线、低价与小盘股后，剩余 {len(df)} 只【主线精锐】等待 K 线演算。")
            else:
                print(f"   -> ⚔️ 盲扫模式启动，剩余 {len(df)} 只标的等待演算。")
            return df
        except Exception as e:
            print(f"   -> ⚠️ 市场快照获取失败 ({e})，正在重试...")
            time.sleep(2)
    return pd.DataFrame()

# ==========================================
# STEP3 K线获取 (内置防爆盾与 0量清理)
# ==========================================
def get_kline(code):
    for attempt in range(3):
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
            if df is None or df.empty: return None
            
            # 🛡️ 盘前防误杀：如果在早盘9:25前运行，最后一根K线成交量为0，自动剔除使用昨日数据！
            while len(df) > 0 and df.iloc[-1]["成交量"] == 0:
                df = df.iloc[:-1]
                
            if len(df) < 250:
                return None
            return df
        except Exception:
            time.sleep(1 + random.random() * attempt)
    return None

# ==========================================
# STEP4 RS与RSI计算引擎
# ==========================================
def calc_rs(df):
    close = df["收盘"].values
    r20 = (close[-1] - close[-21]) / close[-21]
    r60 = (close[-1] - close[-61]) / close[-61]
    r120 = (close[-1] - close[-121]) / close[-121]
    rs = r20*0.4 + r60*0.3 + r120*0.3
    return rs

def calc_rsi(prices):
    delta = np.diff(prices)
    gain = np.maximum(delta, 0)
    loss = np.maximum(-delta, 0)
    avg_gain = gain.mean()
    avg_loss = loss.mean()
    if avg_loss == 0: return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# ==========================================
# STEP5 三大游资战法判定 (核心大脑)
# ==========================================
def analyze_stock(row):
    try:
        code = row["code"]
        name = row["name"]

        df = get_kline(code)
        if df is None: return None

        close = df["收盘"].values
        volume = df["成交量"].values

        ma20 = close[-20:].mean()
        ma50 = close[-50:].mean()
        ma150 = close[-150:].mean()
        ma200 = close[-200:].mean()
        price = close[-1]

        rs = calc_rs(df)
        rsi = calc_rsi(close[-30:])
        vol_ratio = volume[-1] / volume[-50:].mean()

        # -------------------------
        # 1. 右侧突破 (起飞阶段)
        # -------------------------
        breakout = (
            price > ma20 and price > ma50
            and ma50 > ma150 > ma200
            and vol_ratio > 1.5
            and rsi > 60
        )

        # -------------------------
        # 2. 左侧伏击 (缩量踩生命线)
        # -------------------------
        ambush = (
            abs(price - ma20) / ma20 < 0.03
            and vol_ratio < 1.1
            and ma50 > ma150 > ma200
        )

        # -------------------------
        # 3. 黄金坑 (鲁西化工模式：老龙回头)
        # -------------------------
        high60 = close[-60:].max()
        pit = (
            price < high60 * 0.85
            and price >= ma50 * 0.98  
            and vol_ratio > 1.3
        )

        if not (breakout or ambush or pit):
            return None

        if pit: type_label = "🐉 黄金坑"
        elif breakout: type_label = "🔥 突破起飞"
        else: type_label = "🧘 缩量伏击"

        return {
            "Ticker": code,
            "Name": name,
            "Price": round(price, 2),
            "RS_Score": round(rs * 100, 2),
            "RSI": round(rsi, 2),
            "Vol_Ratio": round(vol_ratio, 2),
            "Type": type_label
        }
    except Exception:
        return None

# ==========================================
# STEP6 主扫描调度器
# ==========================================
def scan_market():
    hot_tickers = get_hot_tickers()
    spot = get_market_snapshot(hot_tickers)
    
    if spot.empty: 
        print("❌ 兵力筛选失败，无法获取大盘数据。")
        return[]

    print("\n   -> ⚙️ 正在向数据中心请求 K 线演算（极其安全且迅速）...")
    results =[]
    
    # 将并发控制在 6，完美规避 Akshare 触发东财封禁的风险
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as exe:
        futures =[exe.submit(analyze_stock, row) for _, row in spot.iterrows()]
        for f in concurrent.futures.as_completed(futures):
            res = f.result()
            if res: results.append(res)

    return results

# ==========================================
# STEP7 写入作战指令
# ==========================================
def write_sheet(data):
    print("\n📝 STEP3 正在将绝密作战名单写入 Google Sheets 表格...")
    try:
        sheet = get_worksheet()
        sheet.clear()

        if len(data) == 0:
            sheet.update_acell("A1", "No Signal: 当前战局恶劣，未发现任何符合狙击条件的标的。")
            print("⚠️ 筛选完毕，已写入空仓报告！")
            return

        df = pd.DataFrame(data)
        # 按照 RS 评分（动量强弱）从高到低排序，呈现真正的龙头
        df = df.sort_values("RS_Score", ascending=False)

        sheet.update(values=[df.columns.values.tolist()] + df.values.tolist(), range_name="A1")
        
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sheet.update_acell("H1", "Last Update:")
        sheet.update_acell("I1", now)
        
        print(f"🎉 大功告成！已成功将 {len(df)} 只【主线 + 三大战法】双重认证龙头送达指挥部！")
    except Exception as e:
        print(f"❌ 表格写入致命失败: {e}")
        traceback.print_exc()

# ==========================================
# MAIN
# ==========================================
def main():
    print("\n========== A股猎手系统 V6.1 (东方财富主线双擎版) ==========")
    data = scan_market()
    write_sheet(data)

if __name__ == "__main__":
    main()
