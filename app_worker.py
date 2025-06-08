# app_worker.py
import boto3
import json
import subprocess
import time

REGION = "ap-northeast-2"

sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# 根據環境設定前綴
REQUEST_QUEUE_URL = f"https://sqs.ap-northeast-2.amazonaws.com/881892165012/request-queue-yjche.fifo"
RESPONSE_QUEUE_URL = f"https://sqs.ap-northeast-2.amazonaws.com/881892165012/response-queue-yjche.fifo"
INPUT_BUCKET = f"input-bucket-yjche"
OUTPUT_BUCKET = f"output-bucket-yjche"

while True:
    print("輪詢 SQS 請求佇列...")
    resp = sqs.receive_message(
        QueueUrl=REQUEST_QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20
    )

    if 'Messages' not in resp:
        continue

    for msg in resp['Messages']:
        receipt_handle = msg['ReceiptHandle']
        body = json.loads(msg['Body'])

        # 擷取圖片資訊
        image_key = body['image_s3_key']
        local_path = f"/tmp/{image_key}"

        # 從 S3 下載圖片
        print(f"從 S3 下載圖像: {image_key}")
        s3.download_file(INPUT_BUCKET, image_key, local_path)

        # 呼叫模型分類腳本
        print(f"執行模型推論: {local_path}")
        result = subprocess.check_output(["python3", "/home/ubuntu/classifier/image_classification.py", local_path])
        label = result.decode().strip()

        print(f"推論結果: {label}")

        # 將推論結果存入 S3 輸出桶
        output_key = f"{image_key.split('.')[0]}_result.txt" # 例如，使用原始圖片 UUID 作為文件名
        output_content = json.dumps({
            "original_image_key": image_key,
            "recognition_result": label,
            "timestamp": time.time()
        }) # 建議存儲為 JSON 格式
        try:
            s3.put_object(
                Bucket=OUTPUT_BUCKET, 
                Key=output_key,
                Body=output_content.encode('utf-8'),
                ContentType='application/json' # 根據內容類型設置
            )
            print(f"已將辨識結果 '{output_key}' 儲存至 S3 輸出桶 '{OUTPUT_BUCKET}'。")
        except Exception as e:
            print(f"儲存結果至 S3 輸出桶失敗: {str(e)}")
            # 這裡需要更強健的錯誤處理，例如將訊息發送到死信隊列 (DLQ)
            # 或者不刪除 SQS 請求訊息，以便後續重試
            # 為了避免無限循環，如果 S3 寫入失敗，應該考慮不發送 SQS 響應
            continue # 跳過當前訊息的處理，不發送響應，也不刪除請求訊息

        # 傳送推論結果到 Response Queue
        # 這裡可以考慮只傳送 S3 輸出桶的 Key，而不是完整的結果，因為結果已經在 S3 中了
        response_body = {
            "original_image_key": image_key,
            "recognition_result": label, # 也可以是 output_key
            "result_s3_key": output_key # 添加指向 S3 輸出結果的 Key
        }
        
        sqs.send_message(
            QueueUrl=RESPONSE_QUEUE_URL,
            MessageBody=json.dumps(response_body),
            MessageGroupId="default"
        )

        # 刪除已處理訊息
        sqs.delete_message(QueueUrl=REQUEST_QUEUE_URL, ReceiptHandle=receipt_handle)
        print("訊息處理完成並已刪除")
