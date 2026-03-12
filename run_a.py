import akshare as ak
import pandas as pd
import numpy as np
import concurrent.futures
import datetime
import time
import random
import gspread
from google.oauth2.service_account import Credentials

# =============================
# Google Sheet
# =============================

SHEET_URL="YOUR_SHEET"

creds=Credentials.from_service_account_file(
"credentials.json",
scopes=[
"https://www.googleapis.com/auth/spreadsheets",
"https://www.googleapis.com/auth/drive"
]
)

client=gspread.authorize(creds)

def get_sheet():

    doc=client.open_by_url(SHEET_URL)

    try:
        return doc.worksheet("LeaderBoard")

    except:

        return doc.add_worksheet(
        title="LeaderBoard",
        rows=200,
        cols=20
        )


# =============================
# 防封
# =============================

def fetch_safe(func):

    for i in range(3):

        try:
            return func()

        except:

            time.sleep(2+random.random())

    return None


# =============================
# 市场快照
# =============================

def get_market():

    df=fetch_safe(lambda: ak.stock_zh_a_spot_em())

    df=df.rename(columns={
    "代码":"code",
    "名称":"name",
    "最新价":"price",
    "总市值":"mktcap"
    })

    df=df[["code","name","price","mktcap"]]

    df["price"]=pd.to_numeric(df["price"])
    df["mktcap"]=pd.to_numeric(df["mktcap"])

    df=df[
    (df.price>5)
    &
    (df.mktcap>10000000000)
    ]

    return df


# =============================
# K线
# =============================

cache={}

def get_kline(code):

    if code in cache:

        return cache[code]

    try:

        df=ak.stock_zh_a_hist(
        symbol=code,
        start_date="20230101",
        adjust="qfq"
        )

        if df.empty:

            return None

        cache[code]=df

        return df

    except:

        return None


# =============================
# RS
# =============================

def calc_rs(close):

    c=close[-1]

    r20=(c-close[-20])/close[-20]
    r60=(c-close[-60])/close[-60]
    r120=(c-close[-120])/close[-120]

    return r20*0.4+r60*0.3+r120*0.3


# =============================
# RSI
# =============================

def calc_rsi(prices):

    delta=prices.diff()

    gain=delta.clip(lower=0)
    loss=-delta.clip(upper=0)

    avg_gain=gain.ewm(alpha=1/14).mean()
    avg_loss=loss.ewm(alpha=1/14).mean()

    rs=avg_gain/avg_loss

    rsi=100-(100/(1+rs))

    return rsi.iloc[-1]


# =============================
# 股票分析
# =============================

def analyze(row):

    code=row.code
    name=row.name

    time.sleep(random.uniform(0.1,0.3))

    df=get_kline(code)

    if df is None:

        return None

    close=df["收盘"].values
    vol=df["成交量"].values

    if len(close)<200:

        return None

    price=close[-1]

    ma20=np.mean(close[-20:])
    ma50=np.mean(close[-50:])
    ma150=np.mean(close[-150:])
    ma200=np.mean(close[-200:])

    rs=calc_rs(close)

    rsi=calc_rsi(pd.Series(close))

    vol_ratio=vol[-1]/np.mean(vol[-50:])

    high250=max(close[-250:])

    dist20=(price-ma20)/ma20

    breakout=(
    price>max(close[-50:])
    and ma20>ma50>ma150
    and vol_ratio>1.8
    )

    ambush=(
    abs(dist20)<0.03
    and ma50>ma150>ma200
    )

    pit=(
    price<high250*0.85
    and price>ma50
    )

    if not (breakout or ambush or pit):

        return None

    if breakout:
        t="Breakout"
    elif pit:
        t="GoldenPit"
    else:
        t="Pullback"

    return {

    "code":code,
    "name":name,
    "rs":rs,
    "rsi":rsi,
    "vol":vol_ratio,
    "type":t

    }


# =============================
# 主扫描
# =============================

def scan():

    market=get_market()

    results=[]

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as exe:

        futures=[
        exe.submit(analyze,row)
        for _,row in market.iterrows()
        ]

        for f in concurrent.futures.as_completed(futures):

            r=f.result()

            if r:

                results.append(r)

    df=pd.DataFrame(results)

    # RS Ranking

    df["RS_Rank"]=df.rs.rank(pct=True)*100

    # 龙头评分

    df["Score"]=(
    df.RS_Rank*0.4
    +df.vol*10*0.3
    +df.rsi*0.3
    )

    df=df.sort_values("Score",ascending=False)

    return df.head(20)


# =============================
# 写入
# =============================

def write_sheet(df):

    sheet=get_sheet()

    sheet.clear()

    sheet.update(
    [df.columns.tolist()]
    +df.values.tolist()
    )

    sheet.update_acell(
    "H1",
    datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    )


# =============================
# MAIN
# =============================

def main():

    print("A股猎手 V8 启动")

    df=scan()

    write_sheet(df)

    print("完成",len(df))


if __name__=="__main__":

    main()
