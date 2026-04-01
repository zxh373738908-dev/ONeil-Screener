try:
        # 强制将所有数据转换为字符串或基础数字类型，防止 JSON 报错
        clean_matrix = []
        for row in matrix:
            clean_row = [str(item) if item is not None else "" for item in row]
            clean_matrix.append(clean_row)

        # 发送请求
        resp = requests.post(WEBAPP_URL, json=clean_matrix, timeout=15)
        
        if resp.text == "Success":
            print(f"🎉 港股同步完成！捕捉标的: {len(final_list)} 只")
        else:
            print(f"⚠️ 脚本已接收但未成功写入: {resp.text}")
            
    except Exception as e:
        print(f"❌ 同步失败 (网络或URL错误): {e}")
