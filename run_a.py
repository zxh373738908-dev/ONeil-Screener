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
import traceback

warnings.filterwarnings("ignore")

# ==========================================
# Google Sheet
# ==========================================

OUTPUT_SHEET_URL = "YOUR_GOOGLE_SHEET_URL"

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)


def get_worksheet():
    doc = client.open_by_url(OUTPUT_SHEET_URL)
    try:
        return doc.worksheet("LeaderBoard")
    except:
        return doc.add_worksheet(title="LeaderBoard", rows=200, cols=20)


# ==========================================
# 防封 fetch
# ==========================================

def safe_fetch(func, retries=3):

    for i in range(retries):
        try:
            return func()
        except:
            time.sleep(2 + random.random())

    return None


# ==========================================
# STEP1 行业强度
# ==========================================

def get_hot_tickers():

    print("\n🔥 STEP1 行业雷达")

    try:

        ind_df = safe_fetch(lambda: ak.stock_board_industry_name_em())

        top_inds = ind_df.sort_values(
            by="涨跌幅",
            ascending=False
        ).head(5)

        sectors = top_inds["板块名称"].tolist()

        print("强势行业:", sectors)

        tickers = set()

        for s in sectors:

            try:

                cons = ak.stock_board_industry_cons_em(symbol=s)

                codes = cons["代码"].astype(str).str.zfill(6)

                tickers.update(codes)

                time.sleep(0.3)

            except:
                pass

        print("主线股票:", len(tickers))

        return list(tickers)

    except:

        print("行业雷达失败 → 全市场扫描")

        return []


# ==========================================
# STEP2 市场过滤
# ==========================================

def get_market_snapshot(hot):

    print("\n🚀 STEP2 市场过滤")

    df = safe_fetch(lambda: ak.stock_zh_a_spot_em())

    df = df.rename(columns={
        "代码": "code",
        "名称": "name",
        "最新价": "price",
        "总市值": "mktcap"
    })

    df = df[["code", "name", "price", "mktcap"]]

    df["price"] = pd.to_numeric(df["price"])
    df["mktcap"] = pd.to_numeric(df["mktcap"])

    # 欧奈尔过滤
    df = df[
        (df.price > 5) &
        (df.mktcap > 10000000000)
    ]

    if hot:

        df = df[df["code"].isin(hot)]

    else:

        df = df.sort_values(
            by="mktcap",
            ascending=False
        ).head(300)

    print("候选股票:", len(df))

    return df


# ==========================================
# K线
# ==========================================

k_cache = {}


def get_kline(code):

    if code in k_cache:
        return k_cache[code]

    try:

        df = ak.stock_zh_a_hist(
            symbol=code,
            start_date="20220101",
            adjust="qfq"
        )

        if df.empty:
            return None

        while len(df) > 0 and df.iloc[-1]["成交量"] == 0:
            df = df.iloc[:-1]

        k_cache[code] = df

        return df

    except:

        return None


# ==========================================
# RS
# ==========================================

def calc_rs(close):

    r20 = (close[-1] - close[-20]) / close[-20]
    r60 = (close[-1] - close[-60]) / close[-60]
    r120 = (close[-1] - close[-120]) / close[-120]

    return r20*0.4 + r60*0.3 + r120*0.3


# ==========================================
# Wilder RSI
# ==========================================

def calc_rsi(prices, period=14):

    delta = prices.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/period).mean()
    avg_loss = loss.ewm(alpha=1/period).mean()

    rs = avg_gain / avg_loss

    rsi = 100 - (100/(1+rs))

    return rsi.iloc[-1]


# ==========================================
# 分析股票
# ==========================================

def analyze_stock(row):

    try:

        code = row["code"]
        name = row["name"]

        time.sleep(random.uniform(0.1, 0.3))

        df = get_kline(code)

        if df is None:
            return None

        close = df["收盘"].values
        volume = df["成交量"].values

        if len(close) < 200:
            return None

        price = close[-1]

        ma20 = np.mean(close[-20:])
        ma50 = np.mean(close[-50:])
        ma150 = np.mean(close[-150:])
        ma200 = np.mean(close[-200:])

        rs = calc_rs(close)

        rsi = calc_rsi(pd.Series(close))

        vol_mean = volume[-50:].mean()

        vol_ratio = volume[-1]/vol_mean if vol_mean > 0 else 1

        high250 = max(close[-250:])

        dist20 = (price - ma20)/ma20

        # 突破
        breakout = (
            price > max(close[-50:])
            and ma20 > ma50 > ma150
            and vol_ratio > 1.8
            and rsi > 65
        )

        # 回踩
        pullback = (
            abs(dist20) < 0.03
            and ma50 > ma150 > ma200
        )

        # 黄金坑
        pit = (
            price < high250*0.85
            and price > ma50
            and vol_ratio > 1.3
        )

        if not (breakout or pullback or pit):
            return None

        if breakout:
            t = "🔥Breakout"
        elif pit:
            t = "🐉GoldenPit"
        else:
            t = "🧘Pullback"

        return {
            "Ticker": code,
            "Name": name,
            "Price": round(price,2),
            "RS": round(rs*100,2),
            "RSI": round(rsi,2),
            "VolRatio": round(vol_ratio,2),
            "Type": t
        }

    except:
        return None


# ==========================================
# 扫描
# ==========================================

def scan_market():

    hot = get_hot_tickers()

    spot = get_market_snapshot(hot)

    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as exe:

        futures = [
            exe.submit(analyze_stock,row)
            for _,row in spot.iterrows()
        ]

        for f in concurrent.futures.as_completed(futures):

            r = f.result()

            if r:
                results.append(r)

    df = pd.DataFrame(results)

    if df.empty:
        return df

    # RS Ranking
    df["RS_Rank"] = df["RS"].rank(pct=True)*100

    # 龙头评分
    df["Score"] = (
        df.RS_Rank*0.5 +
        df.VolRatio*10*0.3 +
        df.RSI*0.2
    )

    df = df.sort_values("Score",ascending=False)

    return df.head(20)


# ==========================================
# 写入表格
# ==========================================

def write_sheet(df):

    sheet = get_worksheet()

    sheet.clear()

    if df.empty:

        sheet.update_acell("A1","No Signal")

        return

    sheet.update(
        [df.columns.tolist()] + df.values.tolist()
    )

    sheet.update_acell(
        "H1",
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    )


# ==========================================
# MAIN
# ==========================================

def main():

    print("\n========== A股猎手 V7 PRO ==========")

    df = scan_market()

    write_sheet(df)

    print("完成:",len(df))


if __name__ == "__main__":

    main()
