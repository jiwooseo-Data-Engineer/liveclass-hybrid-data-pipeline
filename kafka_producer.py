# 파이썬 카프카 라이브러리 설치 필요: pip install kafka-python
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_realtime_sale(sale_event):
    print(f"📡 [Kafka Producer] 실시간 이벤트 발행: {sale_event['order_id']}")
    producer.send('liveclass-sales', value=sale_event)
    producer.flush()

if __name__ == "__main__":
    # 가상의 실무 실시간 결제 발생 예시
    sample_event = {
        "order_id": "ORD_99999", "course_id": "COURSE_MIGYUNG_01", 
        "price": 99000, "payment_status": "COMPLETED", "sale_date": "2026-06-08"
    }
    send_realtime_sale(sample_event)