import pandas as pd
import numpy as np
import requests
import re
import yfinance as yf
import datetime
import logging

# ====== Google Sheets 相关依赖 ======
# 如果未安装，请在终端执行: pip install gspread gspread-formatting
import gspread
from gspread_formatting import *

# ==========================================
# 0. 系统配置与日志初始化
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
pd.options.mode.chained_assignment = None  # 忽略部分 Pandas 警告

# ==========================================
# 1. Google Sheets 初始化 (❗请填入你原有的账号验证代码)
# ==========================================
def init_sheet(https://script.google.com/macros/s/AKfycby1pIM7iO43lcLQpOmi5LCJIn3VN9a0Ilf9amoy1EtQV_GBXJkk_A4PpsrJxKzH7i51/exec):
    """
    初始化并返回 Google Sheets 的工作表对象。
    👉 请将这里的代码替换为你自己原有的 gspread 鉴权逻辑！
    """
    # 示例代码：
    # from oauth2client.service_account import ServiceAccountCredentials
    # scope =['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    # creds = ServiceAccountCredentials.from_json_keyfile_name('你的密钥文件.json', scope)
    # client = gspread.authorize(creds)
    # return client.open("HK-Share Screener").sheet1
    
    raise NotImplementedError("❌ 请先在此处填入你的 init_sheet() 谷歌表格鉴权代码！")

# ==========================================
# 2. 核心量化算法 (高级趋势与动能判定)
# ==========================================
def calculate_advanced_v750(df, hsi_series):
    """
    量化核心逻辑：判断主升浪、相对强度(RS)与成交量异动
    """
    try:
        if len(df) < 150: return None # 剔除次新股或数据不足的股
        
        # 提取收盘价与成交量
        close = df['Close']
        vol = df['Volume']
        
        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ma150 = close.rolling(150).mean()
        vol_ma50 = vol.rolling(50).mean()
        
        current_price = close.iloc[-1]
        current_vol = vol.iloc[-1]
        
        # 1. 趋势多头排列 (Minervini 模板核心)
        trend_bull = (current_price > ma20.iloc[-1]) and \
                     (ma20.iloc[-1] > ma50.iloc[-1]) and \
                     (ma50.iloc[-1] > ma150.iloc[-1])
                     
        # 2. 成交量异动 (今日量 > 50日均量的 1.2倍)
        vol_ratio = current_vol / vol_ma50.iloc[-1] if vol_ma50.iloc[-1] > 0 else 0
        
        # 3. 相对大盘强度 (RS)
        # 个股近60日涨幅 / 恒指近60日涨幅
        stock_ret = current_price / close.iloc[-60] - 1
        hsi_ret = hsi_series.iloc[-1] / hsi_series.iloc[-60] - 1
        rs_raw = stock_ret - hsi_ret
        
        # 综合打分 (成交量动能 + 相对趋势强度)
        score = (vol_ratio * 10) + (rs_raw * 100)
        
        # 判断 Action 标签
        action = "观察"
        if trend_bull and vol_ratio > 1.2 and rs_raw > 0:
            action = "🚀主升浪" if vol_ratio > 2.0 else "👁️奇点突破"
            
        return {
            "is_bull": trend_bull,
            "Action": action,
            "Score": score,
            "rs_raw": rs_raw,
            "Price": round(current_price, 2),
            "Vol_Ratio": round(vol_ratio, 2),
            "RS_Vel": round(rs_raw, 3),
            "Stop": round(current_price * 0.92, 2), # 默认8%止损
            "Shares": int(100000 / current_price),  # 假设10w资金单笔建议股数
            "Tight": "Yes" if close.iloc[-10:].std() / close.iloc[-10:].mean() < 0.05 else "No"
        }
    except Exception as e:
        return None

# ==========================================
# 3. 策略执行主类
# ==========================================
class QuantumScanner:
    def __init__(self):
        self.hsi_p = 0
        self.hsi_ma50 = 0
        self.hsi_series = None
        self.weather = "❄️ 观望"
        self.df_pool = pd.DataFrame()
        
    def fetch_benchmark(self):
        logging.info("1. 正在获取大盘基准...")
        hsi_raw = yf.download("^HSI", period="300d", progress=False)['Close']
        self.hsi_series = hsi_raw.iloc[:,0] if isinstance(hsi_raw, pd.DataFrame) else hsi_raw
        self.hsi_p = self.hsi_series.iloc[-1]
        self.hsi_ma50 = self.hsi_series.rolling(50).mean().iloc[-1]
        self.weather = "☀️ 激进" if self.hsi_p > self.hsi_ma50 else "❄️ 观望"
        logging.info(f"大盘环境: {self.weather} (收盘:{self.hsi_p:.0f}, MA50:{self.hsi_ma50:.0f})")

    def scan_tradingview(self):
        logging.info("2. 正在扫描 TradingView 全市场票池...")
        url = "https://scanner.tradingview.com/hongkong/scan"
        payload = {
            "columns":["name", "description", "close", "market_cap_basic", "sector"],
            "filter":[{"left": "market_cap_basic", "operation": "greater", "right": 8e9}], # 降低门槛至80亿港元
            "range": [0, 1000], # 扩大扫描面，防止漏网之鱼
            "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}
        }
        try:
            resp = requests.post(url, json=payload, timeout=15).json().get('data',[])
            self.df_pool = pd.DataFrame([
                {"code": re.sub(r'[^0-9]', '', d['d'][0]), "sector": d['d'][4] or "其他"} 
                for d in resp
            ])
            logging.info(f"TV粗筛完成，获取到 {len(self.df_pool)} 只基础标的。")
        except Exception as e:
            logging.error(f"TradingView 获取失败: {e}")
            raise

    def process_and_rank(self):
        logging.info("3. 正在拉取 Yahoo Finance 详情并执行量化测算(约需30-60秒)...")
        final_list = []
        tickers =[str(c).zfill(4) + ".HK" for c in self.df_pool['code']]
        
        # 批量下载数据
        data = yf.download(tickers, period="1y", group_by='ticker', progress=False, threads=True, auto_adjust=False)
        available_tickers = data.columns.levels[0] if isinstance(data.columns, pd.MultiIndex) else[]
        
        for t in tickers:
            try:
                if t not in available_tickers: continue
                
                # 清洗停牌或缺失数据
                stock_data = data[t].dropna()
                if len(stock_data) < 100: continue 
                
                res = calculate_advanced_v750(stock_data, self.hsi_series)
                
                if res and res.get('is_bull') and res.get('Action') != "观察":
                    code_raw = t.split('.')[0].lstrip('0')
                    sector = self.df_pool[self.df_pool['code']==code_raw].iloc[0]['sector']
                    res.update({"Ticker": t.split('.')[0], "Sector": sector})
                    final_list.append(res)
            except Exception as e:
                continue

        if not final_list:
            logging.warning("当前无股票满足突破/主升浪条件。")
            return pd.DataFrame()

        res_df = pd.DataFrame(final_list)
        
        logging.info("4. 正在执行动态配额与板块评级...")
        # 分数计算：基础分 + RS相对大盘强度排名分
        res_df['Final_Score'] = res_df['Score'] + res_df['rs_raw'].rank(pct=True)*25
        
        # 动态板块配额：大盘好的时候，每个板块允许上榜 8 只；不好的时候收紧到 3 只防范风险
        sector_limit = 8 if self.weather == "☀️ 激进" else 3 
        
        top_picks = res_df.sort_values(by="Final_Score", ascending=False).groupby('Sector').head(sector_limit)
        return top_picks.head(60)

    def export_results(self, df):
        if df.empty: return
        
        cols =["Ticker", "Action", "Final_Score", "Price", "Shares", "Stop", "Tight", "Vol_Ratio", "RS_Vel", "Sector"]
        output_df = df[cols]
        
        # --- 1. 终端预览打印 ---
        logging.info("5. 结果汇总:")
        now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        print("\n" + "="*85)
        print(f"🏰 V45-V750 量子领袖版 | 环境: {self.weather} | 时间: {now_str}")
        print("="*85)
        print(output_df.to_string(index=False))
        print("="*85)
        
        # --- 2. 写入 Google Sheets ---
        logging.info("6. 正在同步到 Google Sheets (HK-Share Screener)...")
        try:
            sh = init_sheet() 
            sh.clear()

            header = [[f"🏰 V45-V750 量子领袖版 (🚀主升浪防错杀版)", f"环境: {self.weather}", f"刷新: {now_str}", "风控: 单笔风险 0.8% / 板块配额制"]]
            sh.update(range_name="A1", values=header)

            # 写入表头及数据
            sh.update(range_name="A3", values=[cols] + output_df[cols].values.tolist(), value_input_option="USER_ENTERED")

            # 美化格式
            set_frozen(sh, rows=3)
            # A到J共10列
            format_cell_range(sh, 'A3:J3', cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(1,1,1)), backgroundColor=color(0,0,0)))

            # 条件格式设置
            rules = get_conditional_format_rules(sh)
            # 奇点先行 - 紫色高亮
            rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['👁️']),
                                        format=cellFormat(backgroundColor=color(0.9, 0.8, 1), textFormat=textFormat(bold=True)))))
            # 主升浪 / 巅峰突破 - 橙红色高亮
            rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('B4:B100', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('TEXT_CONTAINS', ['🚀']),
                                        format=cellFormat(backgroundColor=color(1.0, 0.9, 0.8), textFormat=textFormat(bold=True, foregroundColor=color(0.8, 0.2, 0.2))))))
            # 建议股数 - 绿色提醒
            rules.append(ConditionalFormatRule(ranges=[GridRange.from_a1_range('E4:E100', sh)],
                booleanRule=BooleanRule(condition=BooleanCondition('NUMBER_GREATER', ['0']),
                                        format=cellFormat(textFormat=textFormat(bold=True, foregroundColor=color(0, 0.5, 0))))))
            rules.save()
            
            logging.info("✅ Google Sheets 同步成功！请前往网页查看。")
            
        except Exception as e:
            logging.error(f"❌ 同步到 Google Sheets 失败: {e}\n(请确保你在 init_sheet() 函数中填入了正确的鉴权代码)")

# ==========================================
# 4. 运行入口
# ==========================================
if __name__ == "__main__":
    scanner = QuantumScanner()
    try:
        scanner.fetch_benchmark()
        scanner.scan_tradingview()
        results = scanner.process_and_rank()
        scanner.export_results(results)
        logging.info("🎉 任务流完全结束。")
    except Exception as e:
        logging.error(f"❌ 程序运行阻断: {e}")
