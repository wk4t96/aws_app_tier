# app_worker.py
import boto3
import os
import json
import subprocess
import time

REGION = "ap-northeast-2"
AWS_ACCESS_KEY_ID=AKIA42VG6QWKJEVY5C64
AWS_SECRET_ACCESS_KEY=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

sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# 你根據環境設定前綴
UNIQUE_PREFIX = os.environ['AWS_ACCESS_KEY_ID'][:8].lower()
REQUEST_QUEUE_URL = f"https://sqs.ap-northeast-2.amazonaws.com/881892165012/request-queue-{UNIQUE_PREFIX}.fifo"
RESPONSE_QUEUE_URL = f"https://sqs.ap-northeast-2.amazonaws.com/881892165012/response-queue-{UNIQUE_PREFIX}.fifo"
INPUT_BUCKET = f"input-bucket-{UNIQUE_PREFIX}"

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

        # 傳送推論結果到 Response Queue
        response_body = {
            "original_image_key": image_key,
            "recognition_result": label
        }
        sqs.send_message(
            QueueUrl=RESPONSE_QUEUE_URL,
            MessageBody=json.dumps(response_body),
            MessageGroupId="default"
        )

        # 刪除已處理訊息
        sqs.delete_message(QueueUrl=REQUEST_QUEUE_URL, ReceiptHandle=receipt_handle)
        print("訊息處理完成並已刪除")
