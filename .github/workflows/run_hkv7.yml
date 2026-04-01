name: HKv7-Share Screener

on:
  schedule:
    # 港股收盘时间通常为北京时间 16:10（包含收盘竞价）。
    # 这里设置为每天 UTC 时间 08:30 运行（即北京时间下午 4:30，确保在港股完全收盘后运行）
    - cron: '30 8 * * 1-5'
  workflow_dispatch:

jobs:
  run-hk-screener:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pandas numpy gspread google-auth yfinance requests scipy gspread-formatting

      - name: Create credentials.json
        run: |
          echo '${{ secrets.GCP_CREDENTIALS }}' > credentials.json

      - name: Run HK-Share Screener
        # 假设你的港股运行脚本名为 run_hkv7.py
        run: python run_hkv7.py
