import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import datetime, time, warnings, logging, requests, os
import yfinance as yf

# --- Optional Formatting Library ---
try:
    from gspread_formatting import *
    HAS_FORMATTING = True
except ImportError:
    HAS_FORMATTING = False

# --- Basic Configuration & Suppress Noise ---
warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ApexScreener')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ==========================================
# 1. Configuration & Schema
# ==========================================
CONFIG = {
    "SS_KEY": "14v3_Rm60BsZtpyAY87urGsqPO00erUQT4lNZJjUDyK8",
    "CREDS_FILE": "credentials.json",
    "TZ": datetime.timezone(datetime.timedelta(hours=8)),
    "SHEET_NAME": "A-v7-screener",
    "MIN_MKT_CAP": 85e8,
    "MIN_DAILY_AMT": 1.5e8,
}

# Unified mapping of Internal Keys -> Display Names
SCHEMA = {
    "code": "代码",
    "name": "名称",
    "display_tag": "勋章",
    "rs_rating": "RS评级",
    "rs_slope": "RS斜率",
    "bias_50": "50日乖离",
    "vdu_signal": "缩量",
    "rrr": "盈亏比",
    "sector_status": "板块地位",
    "industry": "行业",
    "price": "现价",
    "stop_loss": "止损",
    "target_price": "目标"
}

def init_sheet():
    """Securely initialize Google Sheets connection."""
    if not os.path.exists(CONFIG["CREDS_FILE"]):
        logger.error(f"Missing {CONFIG['CREDS_FILE']}. Ensure GitHub Secrets are configured.")
        exit(1)

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_file(CONFIG["CREDS_FILE"], scopes=scopes)
        client = gspread.authorize(creds)
        doc = client.open_by_key(CONFIG["SS_KEY"])
        try:
            return doc.worksheet(CONFIG["SHEET_NAME"])
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"Creating new worksheet: {CONFIG['SHEET_NAME']}")
            return doc.add_worksheet(title=CONFIG["SHEET_NAME"], rows=1000, cols=20)
    except Exception as e:
        logger.critical(f"Google API Authorization Failed: {e}")
        exit(1)

# ==========================================
# 🧠 2. Alpha Apex Engine
# ==========================================
def calculate_apex_metrics(df, idx_df):
    """Core mathematical engine for technical analysis."""
    try:
        if len(df) < 200: return None
        
        c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']
        price = float(c.iloc[-1])
        
        # 1. Liquidity Check
        if (c * v).tail(20).mean() < CONFIG["MIN_DAILY_AMT"]: return None
        
        # 2. Moving Averages & Trend
        ma50 = c.rolling(50).mean().iloc[-1]
        ma200 = c.rolling(200).mean().iloc[-1]
        if price < ma50 or ma50 < ma200: return None

        # 3. Relative Strength (RS) Calculations
        # Weights: 1mo (45%), 3mo (20%), 6mo (20%), 12mo (15%)
        rs_raw = ( (price/c.iloc[-21])*0.45 + (price/c.iloc[-63])*0.2 + 
                   (price/c.iloc[-126])*0.2 + (price/c.iloc[-252])*0.15 )
        
        rs_line = c / idx_df
        rs_slope = (rs_line.iloc[-1] - rs_line.iloc[-6]) / rs_line.iloc[-6] * 100
        is_blue_dot = rs_line.iloc[-1] >= rs_line.tail(250).max() * 0.99

        # 4. Volatility & Volume (VCP Logic)
        tightness = (h.tail(8).max() - l.tail(8).min()) / l.tail(8).min() * 100
        vdu = (v.tail(5) < v.rolling(60).mean().tail(5) * 0.55).any()
        bias_50 = (price / ma50 - 1) * 100

        # 5. Tagging Logic
        tag = "关注"
        if tightness < 4 and vdu and is_blue_dot: tag = "💎 巅峰奇点"
        elif price > c.rolling(10).mean().iloc[-1] > ma50 and is_blue_dot: tag = "🚀 强力主升"
        elif is_blue_dot: tag = "🔹 RS领跑"
        elif tightness < 2.5: tag = "🌪️ 极致紧致"
        
        if tag == "关注" or price < c.iloc[-2] * 0.97: return None 

        # 6. Risk / Reward
        tr = pd.concat([h-l, abs(h-c.shift()), abs(l-c.shift())], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        stop_loss = price - (atr * 1.6)
        target_price = h.tail(250).max() * 1.1
        rrr = (target_price - price) / (price - stop_loss + 0.01)

        return {
            "tag": tag, "rs_raw": rs_raw, "rs_slope": round(rs_slope, 2),
            "bias_50": round(bias_50, 1), "vdu_signal": "✅" if vdu else "❌", 
            "rrr": round(rrr, 1), "stop_loss": round(stop_loss, 2), 
            "target_price": round(target_price, 2), "is_ext": bias_50 > 22
        }
    except Exception:
        return None

# ==========================================
# 🚀 3. Orchestrator
# ==========================================
def run_screener():
    now_str = datetime.datetime.now(CONFIG["TZ"]).strftime('%Y-%m-%d %H:%M')
    logger.info("Starting Apex Screener V52.9 (Refactored)")

    # A. Market Benchmark
    try:
        idx_data = yf.download("000300.SS", period="400d", progress=False)['Close']
        idx_series = idx_data.iloc[:, 0] if isinstance(idx_data, pd.DataFrame) else idx_data
    except Exception as e:
        logger.error(f"Failed to fetch Index: {e}"); return

    # B. Universe Filtering (TradingView)
    tv_url = "https://scanner.tradingview.com/china/scan"
    payload = {
        "columns": ["name", "description", "market_cap_basic", "industry", "close"],
        "filter": [{"left": "market_cap_basic", "operation": "greater", "right": CONFIG["MIN_MKT_CAP"]}],
        "range": [0, 800], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
    }
    try:
        resp = requests.post(tv_url, json=payload, timeout=15).json().get('data', [])
        pool = pd.DataFrame([{"code": d['d'][0], "name": d['d'][1], "mkt": d['d'][2], "industry": d['d'][3], "price": d['d'][4]} for d in resp])
    except Exception as e:
        logger.error(f"TradingView API Error: {e}"); return

    # C. Technical Analysis (Batched)
    all_results = []
    tickers = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in pool['code']]
    chunk_size = 50
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        logger.info(f"Processing Chunk {i//chunk_size + 1}/{len(tickers)//chunk_size + 1}...")
        data = yf.download(chunk, period="2y", group_by='ticker', progress=False, threads=True)
        
        for t in chunk:
            try:
                hist = data[t].dropna()
                if hist.empty: continue
                
                row_info = pool[pool['code'] == t.split('.')[0]].iloc[0]
                metrics = calculate_apex_metrics(hist, idx_series)
                
                if metrics:
                    metrics.update({
                        "code": t.split('.')[0], "name": row_info['name'], 
                        "industry": row_info['industry'], "price": row_info['price'],
                        "display_tag": "⚠️ 极致乖离" if metrics['is_ext'] else metrics['tag']
                    })
                    all_results.append(metrics)
            except Exception: continue

    # D. Ranking & Formatting
    sh = init_sheet()
    if not all_results:
        sh.clear()
        sh.update_acell("A1", f"⚠️ No valid signals found on {now_str}. Market breadth may be low.")
        return

    final_df = pd.DataFrame(all_results)
    final_df['rs_rating'] = final_df['rs_raw'].rank(pct=True).apply(lambda x: int(x*99))
    
    # Sector Leader Logic
    ind_avg = final_df.groupby("industry")['rs_rating'].transform('mean')
    final_df['sector_status'] = ind_avg.apply(lambda x: "🔥 领涨主线" if x > 85 else "普通趋势")

    # Final Filter & Sort
    processed_df = (final_df[final_df['rs_rating'] > 70]
                    .sort_values("rs_rating", ascending=False)
                    .groupby("industry").head(5).head(65))

    # E. Output Mapping
    display_df = processed_df.rename(columns=SCHEMA)
    final_cols = [SCHEMA[k] for k in SCHEMA if SCHEMA[k] in display_df.columns]
    
    sh.clear()
    sh.update(range_name="A1", 
              values=[final_cols] + display_df[final_cols].values.tolist(), 
              value_input_option="USER_ENTERED")
    sh.update_acell("N1", f"Apex V52.9 | {now_str} | Breadth: {len(all_results)} hits")

    # F. Visual Formatting (Conditional)
    if HAS_FORMATTING:
        try:
            # Freeze header
            set_frozen(sh, rows=1)
            # Color RS Rating Column (usually Col D)
            fmt_range = f"D2:D{len(processed_df)+1}"
            rule = ConditionalFormatRule(
                ranges=[GridRange.from_a1_range(fmt_range, sh)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition('NUMBER_GREATER', ['90']),
                    format=cellFormat(backgroundColor=color(1, 0.9, 0.6), textFormat=textFormat(bold=True))
                )
            )
            get_conditional_format_rules(sh).append(rule).save()
        except Exception: pass

    logger.info(f"Screener completed successfully. {len(processed_df)} stocks pushed.")

if __name__ == "__main__":
    run_screener()
