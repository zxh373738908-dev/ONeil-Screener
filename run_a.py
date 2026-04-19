import yfinance as yf
import pandas as pd
import numpy as np
import datetime, requests, json, warnings, uuid
from datetime import timezone, timedelta

warnings.filterwarnings('ignore')

# ==========================================
# 1. 配置中心与行业映射
# ==========================================
WEBAPP_URL = "https://script.google.com/macros/s/AKfycbyYfpfYNyRhXcyZrfIHEyErECMM82xkCKfZm71RUZ1YL6Xjr5Kca3ruoVJzxcNAwH9q/exec"

# 设定自定义基准日期（例如用于计算 2025 YTD 涨幅，这里用 2024年最后一个交易日）
BASE_DATE = "2024-12-31" 

SECTOR_MAP = {
    "300502": "半导体", "300308": "半导体", "300394": "半导体", "688313": "半导体", "688041": "半导体", "603501": "半导体",
    "300750": "新能源", "002594": "新能源", "002475": "苹果链/电子", "002371": "特高压/电子",
    "600519": "白酒消费", "000333": "家电消费", "000951": "汽车零部件",
    "601899": "有色资源", "601857": "石油石化", "601208": "工业金属", "600105": "永磁/资源",
    "600030": "金融证券", "002428": "综合/航运", "003031": "智能制造", 
    "601138": "算力/工业富联", "603259": "医疗/药明", "002222": "猪肉/养殖", "603799": "锂电/材料"
}

CORE_TICKERS_RAW = list(SECTOR_MAP.keys())

def format_ticker(code):
    c = str(code).zfill(6)
    return f"{c}.SS" if c.startswith('6') else f"{c}.SZ"

def safe_convert(obj):
    if isinstance(obj, (np.integer, np.floating)): 
        return float(obj) if not np.isnan(obj) else 0.0
    return str(obj)

# ==========================================
# 2. 核心分析引擎 (完全匹配新字段)
# ==========================================
def analyze_v25(data, bench_series, tickers_raw):
    all_results = []
    
    # 辅助函数：安全计算周期涨幅
    def get_ret(ser, d): 
        if len(ser) < 2: return 0.0
        safe_d = min(len(ser) - 1, d)
        return (ser.iloc[-1] / ser.iloc[-safe_d - 1]) - 1

    for t_raw in tickers_raw:
        t_full = format_ticker(t_raw)
        try:
            df = data[t_full].ffill().dropna()
            if len(df) < 20: continue
            
            c = df['Close']
            curr_price = float(c.iloc[-1])
            
            # 1. 1D% (今日涨幅)
            ret_1d = (c.iloc[-1] / c.iloc[-2] - 1) * 100
            
            # 2. 60-Day Trend (60日均线乖离率Bias)
            if len(c) >= 60:
                ma60 = c.rolling(60).mean().iloc[-1]
                trend_60 = ((curr_price - ma60) / ma60) * 100
            else:
                trend_60 = 0.0

            # 3. 绝对涨幅 R (20, 60, 120)
            r5 = get_ret(c, 5) * 100
            r20 = get_ret(c, 20) * 100
            r60 = get_ret(c, 60) * 100
            r120 = get_ret(c, 120) * 100
            
            # 4. 基准涨幅 Bench R
            br5 = get_ret(bench_series, 5) * 100
            br20 = get_ret(bench_series, 20) * 100
            br60 = get_ret(bench_series, 60) * 100
            br120 = get_ret(bench_series, 120) * 100
            
            # 5. 相对涨幅 REL (Relative to Benchmark)
            rel5 = r5 - br5
            rel20 = r20 - br20
            rel60 = r60 - br60
            rel120 = r120 - br120
            
            # 6. 计算 Base Date 至今的涨幅 (例如 2025 YTD)
            try:
                target_dt = pd.to_datetime(BASE_DATE).tz_localize(c.index.tz) if c.index.tz else pd.to_datetime(BASE_DATE)
                past_data = c[c.index <= target_dt]
                if not past_data.empty:
                    base_price = past_data.iloc[-1]
                    ret_base = (curr_price / base_price - 1) * 100
                else:
                    ret_base = 0.0
            except:
                ret_base = 0.0

            # 7. RS_Raw 评分计算 (用于计算 Rank)
            rs_score = rel20 * 0.4 + rel60 * 0.3 + rel120 * 0.3 + 100
            
            all_results.append({
                "Code": t_raw, 
                "Price": curr_price, 
                "1D": f"{ret_1d:+.2f}%",
                "60D_Trend": f"{trend_60:+.2f}%",
                "R20": f"{r20:+.2f}%", "R60": f"{r60:+.2f}%", "R120": f"{r120:+.2f}%",
                "REL5": f"{rel5:+.2f}%", "REL20": f"{rel20:+.2f}%", 
                "REL60": f"{rel60:+.2f}%", "REL120": f"{rel120:+.2f}%",
                "Base_Ret": f"{ret_base:+.2f}%",
                "RS_Raw": rs_score
            })
        except Exception as e:
            continue
    
    return all_results

# ==========================================
# 3. 主流程
# ==========================================
def main():
    tz = timezone(timedelta(hours=8))
    dt_str = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    trace_id = f"RPS-{uuid.uuid4().hex[:4].upper()}"
    
    print(f"🚀 V60.25 相对强度面板启动 | ID: {trace_id}")
    tickers_full = [format_ticker(t) for t in CORE_TICKERS_RAW]
    
    try:
        # 下载数据，拉长到 1年 以确保够计算 R120 和获取年底基准价格
        data = yf.download(tickers_full, period="1y", group_by='ticker', progress=False, auto_adjust=True)
        idx = yf.download("000300.SS", period="1y", progress=False, auto_adjust=True)
        bench = idx['Close'].ffill().iloc[:,0] if isinstance(idx['Close'], pd.DataFrame) else idx['Close'].ffill()
    except Exception as e:
        print(f"❌ 数据获取失败: {e}"); return

    # 执行分析
    analysis_list = analyze_v25(data, bench, CORE_TICKERS_RAW)

    # 计算 RS_Rank
    analysis_list.sort(key=lambda x: x['RS_Raw'], reverse=True)
    total = len(analysis_list)
    for i, item in enumerate(analysis_list):
        item['Rank'] = int((total - i) / total * 100) # 转换为 1-99 的百分位Rank

    # ==========================================
    # 构建严格匹配用户要求的表格表头及输出排序
    # ==========================================
    header = [
        # 第一行 (13列，用于记录元数据)
        ["📊 V60.25 趋势强度面板", "ID:", trace_id, "模式:", "RPS & Momentum", "更新:", dt_str, "", "", "", "", "", ""], 
        # 第二行 (严格按照要求的字段顺序)
        ["代码", "Price", "1D%", "60-Day Trend", "R20", "R60", "R120", "Rank", "REL5", "REL20", "REL60", "REL120", f"From {BASE_DATE}"]
    ]
    
    rows = []
    for x in analysis_list:
        rows.append([
            x['Code'], 
            x['Price'], 
            x['1D'], 
            x['60D_Trend'], 
            x['R20'], 
            x['R60'], 
            x['R120'], 
            x['Rank'], 
            x['REL5'], 
            x['REL20'], 
            x['REL60'], 
            x['REL120'], 
            x['Base_Ret']
        ])
    
    try:
        payload = json.loads(json.dumps(header + rows, default=safe_convert))
        resp = requests.post(WEBAPP_URL, json=payload, timeout=30)
        print(f"📡 结果已推送 | Google 响应: {resp.text}")
    except: 
        print("❌ 推送失败")

if __name__ == "__main__":
    main()
