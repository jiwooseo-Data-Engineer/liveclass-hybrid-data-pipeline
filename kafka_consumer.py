from kafka import KafkaConsumer
import json

print("Kafka Consumer 실시간 로그 수집 세션")

consumer = KafkaConsumer(
    'liveclass-sales',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# 토픽에 데이터가 들어올 때마다 대기하고 있다가 실시간으로 루프를 돌며 처리함
for message in consumer:
    event_data = message.value
    print(f"🔥 [실시간 감지] 수집된 데이터: {event_data['order_id']} - {event_data['course_id']}")
    
    # 💡 실무 확장 포인트: 
    # 여기서 바로 BigQuery 실시간 스트리밍 API로 밀어 넣거나, 
    # 일별 마크 가공기인 01_task_course_analytics_MTD.py의 입력 재료로 전달합니다.