try:
        clean_matrix = json.loads(json.dumps(matrix, default=lambda x: safe_val(x, is_num=False)))
        resp = requests.post(WEBAPP_URL, json=clean_matrix, timeout=15)
        print(f"服务器返回信息: {resp.text}") # <--- 添加这一行
        print(f"🎉 A股数据同步成功！耗时: {round(time.time() - start_time, 2)}s"
