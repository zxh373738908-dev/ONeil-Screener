import pandas as pd

import numpy as np

import gspread

from google.oauth2.service_account import Credentials

import datetime

import warnings

import yfinance as yf

import requests

import re

import time

import sys

from gspread_formatting import *

warnings.filterwarnings('ignore')

# ==========================================

# 1. 配置中心

# ==========================================

SS_KEY = "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8"

TARGET_GID = 665566258  

CREDS_FILE = "credentials.json"

ACCOUNT_SIZE = 1000000 

MAX_RISK_PER_TRADE = 0.008 

TZ_SHANGHAI = datetime.timezone(datetime.timedelta(hours=8))

# 核心保底池：如果 API 失效，强制审计这些核心领袖

FALLBACK_TICKERS = [

    "0700", "3690", "9988", "1810", "1211", "9888", "2318", "0941", "0388", "0005",

    "1024", "9618", "2015", "2269", "1177", "0857", "0883", "0386", "1398", "0939",

    "3988", "2628", "2319", "0992", "2020", "2331", "1088", "1880", "6030", "3968",

    "0267", "0016", "0002", "0003", "1928", "1113", "0011", "0001", "0960", "1093"

]

def init_commander_sheet():

    try:

        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)

        client = gspread.authorize(creds)

        doc = client.open_by_key(SS_KEY)

        for ws in doc.worksheets():

            if str(ws.id) == str(TARGET_GID): return ws

        return doc.get_worksheet(0)

    except Exception as e:

        print(f"❌ Google Sheets 初始化失败: {e}")

        sys.exit(1)

def get_chinese_names(codes):

    mapping = {}

    if not codes: return mapping

    try:

        for i in range(0, len(codes), 50):

            chunk = [f"hk{str(c).zfill(5)}" for c in codes[i:i+50]]

            url = f"http://qt.gtimg.cn/q={','.join(chunk)}"

            r = requests.get(url, timeout=10)

            matches = re.findall(r'v_hk(\d+)="[^]+([^~]+)', r.text)

            for c, n in matches: mapping[str(c).lstrip('0')] = n

    except: pass

    return mapping

# ==========================================

# 🧠 2. 统帅核心算法

# ==========================================

def calculate_commander_signals(df, hsi_series, hstech_ok):

    try:

        df = df.dropna(subset=['Close'])

        if len(df) < 150: return "DATA_SHORT"

        close = df['Close'].values.astype(float)

        high = df['High'].values.astype(float)

        low = df['Low'].values.astype(float)

        vol = df['Volume'].values.astype(float)

        cp = close[-1]

        avg_turn_20 = np.mean((close * vol)[-20:])

        if avg_turn_20 < 60000000: return "LOW_LIQUID"

        ma50 = np.mean(close[-50:])

        dist_ma50 = (cp / ma50 - 1) * 100

        common_idx = hsi_series.index.intersection(df.index)

        rs_line = close[df.index.get_indexer(common_idx)] / hsi_series.loc[common_idx].values

        rs_nh = rs_line[-1] >= np.max(rs_line[-252:])

        price_bins = np.linspace(np.min(low[-100:]), np.max(high[-100:]), 40)

        hist, edges = np.histogram(close[-100:], bins=price_bins, weights=vol[-100:])

        poc_price = edges[np.argmax(hist)]

        tightness = (np.std(close[-10:]) / np.mean(close[-10:])) * 100

        vdu = vol[-1] < np.mean(vol[-20:]) * 0.6

        neg_vol = vol[-11:-1][close[-11:-1] < close[-12:-2]]

        is_pocket = (close[-1] > close[-2]) and (vol[-1] > (np.max(neg_vol) if len(neg_vol)>0 else 0))

        signals = []

        if rs_nh and cp < np.max(close[-20:]) * 1.025: signals.append("奇點")

        if cp > ma50 and dist_ma50 < 4.0 and (vdu or tightness < 1.3): signals.append("老龍")

        if cp >= np.max(close[-20:]) and vol[-1] > np.mean(vol[-20:]) * 1.2: signals.append("突破")

        if rs_nh and hstech_ok and cp > poc_price: signals.append("共振")

        if not signals: return "NO_SIGNAL"

        action = f"💎 統帥共振" if len(signals) >= 3 else (f"🔥 双重({'+'.join(signals)})" if len(signals)==2 else f"🚀 {signals[0]}")

        if dist_ma50 > 15: action = "⚠️ 乖離過大"

        adr = np.mean((high[-20:]-low[-20:])/close[-20:]) * 100

        stop = max(ma50  0.985, cp  (1 - adr0.011.6))

        shares = (ACCOUNT_SIZE * MAX_RISK_PER_TRADE) // (cp - stop) if cp > stop else 0

        return {

            "Action": action, "Price": cp, "Dist_50": round(dist_ma50, 1),

            "Shares": int(shares), "Stop": round(stop, 2), "Tight": round(tightness, 2),

            "Above_POC": "✅" if cp > poc_price else "-", "rs_raw": cp/close[-120] if len(close)>120 else 1,

            "Score": 60 + len(signals)*10, "is_stage_2": cp > ma50

        }

    except: return "ERROR"

# ==========================================

# 🚀 3. 执行主逻辑

# ==========================================

def main():

    now_str = datetime.datetime.now(TZ_SHANGHAI).strftime('%m-%d %H:%M')

    print(f"[{now_str}] 🛰️ V45 量子统帅启动 (增强探测模式)...")

    sh = init_commander_sheet()

    sh.update(range_name="A1", values=[[f"🏯 正在审计...", f"心跳: {now_str}", "状态: 获取票池中..."]])

    # 1. 环境审计

    try:

        mkt_data = yf.download(["^HSI", "3088.HK"], period="60d", progress=False)['Close']

        hsi_series = mkt_data["^HSI"].dropna()

        hstech_ok = mkt_data["3088.HK"].iloc[-1] > mkt_data["3088.HK"].rolling(20).mean().iloc[-1]

    except:

        print("❌ 指数下载失败"); return

    # 2. 获取票池 (增加伪装与保底)

    all_codes = FALLBACK_TICKERS.copy()

    try:

        url = "https://scanner.tradingview.com/hongkong/scan"

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

        payload = {"columns": ["name", "sector"], "filter": [{"left": "market_cap_basic", "operation": "greater", "right": 1.2e10}], "range": [0, 200], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}}

        resp = requests.post(url, json=payload, headers=headers, timeout=15)

        if resp.status_code == 200:

            data_tv = resp.json().get('data', [])

            all_codes += [re.sub(r'[^0-9]', '', d['d'][0]) for d in data_tv]

            print(f"✅ TV 接口调用成功，获取标的: {len(data_tv)}")

        else:

            print(f"⚠️ TV 接口返回状态码 {resp.status_code}，启动保底核心池")

    except Exception as e:

        print(f"⚠️ 票池接口异常: {e}，启动保底核心池")

    all_codes = list(set(all_codes))

    tickers = [c.zfill(4)+".HK" for c in all_codes]

    name_map = get_chinese_names(all_codes)

    # 3. 批量下载与审计

    print(f"🔎 正在审计 {len(tickers)} 只标的形态...")

    data = yf.download(tickers, period="2y", group_by='ticker', progress=False, threads=True)

    final_list = []

    for t in tickers:

        try:

            if t not in data.columns.levels[0]: continue

            res = calculate_commander_signals(data[t], hsi_series, hstech_ok)

            if isinstance(res, dict) and res['is_stage_2']:

                code_clean = t.split('.')[0].lstrip('0')

                res.update({"Ticker": t.split('.')[0], "Name": name_map.get(code_clean, t)})

                final_list.append(res)

        except: continue

    # 4. 写入 Google Sheets

    sh.clear()

    sh.update(range_name="A1", values=[[f"🏯 量子统帅旗舰版", f"环境: {'☀️激进' if hstech_ok else '☁️谨慎'}", f"刷新: {now_str}", f"有效信号: {len(final_list)}"]])

    if final_list:

        res_df = pd.DataFrame(final_list)

        res_df['RS_Rank'] = res_df['rs_raw'].rank(pct=True).apply(lambda x: int(x*99))

        cols = ["Ticker", "Name", "Action", "RS_Rank", "Price", "Shares", "Stop", "Tight", "Dist_50"]

        sh.update(range_name="A3", values=[cols] + res_df.sort_values(by="Score", ascending=False).head(50)[cols].values.tolist(), value_input_option="USER_ENTERED")

        set_frozen(sh, rows=3)

        print(f"✅ 看板已同步，发现 {len(final_list)} 个信号。")

    else:

        sh.update_acell("A4", "📭 审计完成：当前环境无符合统帅形态标的")

        print("📭 审计完成：无符合形态标的")

if __name__ == "__main__":

    main()
