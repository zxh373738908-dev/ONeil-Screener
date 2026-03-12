import akshare as ak
import pandas as pd
import numpy as np
import datetime
import concurrent.futures
import gspread
from google.oauth2.service_account import Credentials
import time
import random

# ==========================================
# Google Sheet 配置
# ==========================================
OUTPUT_SHEET_URL = "YOUR_SHEET"

scopes =[
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(
    "credentials.json",
    scopes=scopes
)
client = gspread.authorize(creds)

# ==========================================
# STEP1: 同花顺主线雷达 (抓取当日最强风口)
# ==========================================
def get_ths_hot_tickers():
    print("\n🔥 STEP1 启动【同花顺】主线雷达，扫描今日最强资金风口...")
    try:
        # 获取同花顺所有行业板块实时数据
        ind_df = ak.stock_board_industry_name_ths()
        
        # 动态匹配列名（防 akshare 更新变动）
        name_col = next((c for c in ind_df.columns if '板块' in c or '名' in c), None)
        pct_col = next((c for c in ind_df.columns if '涨跌幅' in c or '幅' in c), None)
        
        ind_df[pct_col] = pd.to_numeric(ind_df[pct_col], errors='coerce')
        
        # 挑选今天涨幅最猛的前 5 大同花顺板块
        top_inds = ind_df.sort_values(by=pct_col, ascending=False).head(5)
        print(f"   -> ✅ 锁定同花顺前 5 大强势行业：{top_inds[name_col].tolist()}")
        
        hot_tickers = set()
        for _, row in top_inds.iterrows():
            ind_name = row[name_col]
            try:
                # 获取该板块下的所有成分股
                cons_df = ak.stock_board_industry_cons_ths(symbol=ind_name)
                code_col = next((c for c in cons_df.columns if '代码' in c), None)
                codes =[str(x).zfill(6) for x in cons_df[code_col].tolist()]
                hot_tickers.update(codes)
                time.sleep(0.5)  # 礼貌停顿，防同花顺反爬
            except Exception as e:
                print(f"   -> ⚠️ 板块 {ind_name} 成分股获取波动: {e}")
                
        print(f"   -> 🎯 同花顺雷达扫描完毕，共提取 {len(hot_tickers)} 只主线热门标的！")
        return list(hot_tickers)
    except Exception as e:
        print(f"   -> ❌ 同花顺雷达受阻 ({e})，系统将自动降级为全市场盲扫模式。")
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
            
            # 基础防坑过滤：过滤低价仙股和 40亿以下极小盘
            df = df[df["price"] > 5]
            df = df[df["mktcap"] > 4000000000]
            
            # 【核心护盾】：如果同花顺主线获取成功，则剔除非主线股票！
            if hot_tickers:
                df = df[df["code"].isin(hot_tickers)]
                print(f"   -> ⚔️ 剔除非主线、低价与小盘股后，剩余 {len(df)} 只【同花顺认证精锐】等待 K 线演算。")
            else:
                print(f"   -> ⚔️ 盲扫模式启动，剩余 {len(df)} 只标的等待演算。")
            return df
        except Exception:
            time.sleep(2)
    return pd.DataFrame()

# ==========================================
# STEP3 K线获取 (内置防爆盾与 0量清理)
# ==========================================
def get_kline(code):
    for attempt in range(3):
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
            
            # 🛡️ 盘前防误杀：如果在早盘9:20前运行，最后一根K线成交量为0，自动剔除使用昨日数据！
            while len(df) > 0 and df.iloc[-1]["成交量"] == 0:
                df = df.iloc[:-1]
                
            if len(df) < 250:
                return None
            return df
        except Exception:
            # 遇到防火墙拦截，随机指数退避
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
            and price >= ma50 * 0.98  # 允许极度轻微刺破50日线
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
            "Price": price,
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
    # 1. 先用同花顺锁定主线
    ths_hot_tickers = get_ths_hot_tickers()
    # 2. 拉取全大盘并进行兵力过滤
    spot = get_market_snapshot(ths_hot_tickers)
    
    if spot.empty: return[]

    print("\n   -> ⚙️ 正在向数据中心请求 K 线演算（由于请求量已大幅降低，此过程极其安全且迅速）...")
    results =[]
    
    # 降低并发到 5，配合降低后的基数，绝对不会触发 Github IP 封锁
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as exe:
        futures = [exe.submit(analyze_stock, row) for _, row in spot.iterrows()]
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
        sheet = client.open_by_url(OUTPUT_SHEET_URL).worksheet("A-Share Screener")
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
        sheet.update_acell("I1", "Last Update:")
        sheet.update_acell("J1", now)
        
        print(f"🎉 大功告成！已成功将 {len(df)} 只【同花顺主线 + 三大战法】双重认证龙头送达指挥部！")
    except Exception as e:
        print(f"❌ 表格写入失败: {e}")

# ==========================================
# MAIN
# ==========================================
def main():
    print("\n========== A股猎手系统 V6 (同花顺主线双擎版) ==========")
    data = scan_market()
    write_sheet(data)

if __name__ == "__main__":
    main()
