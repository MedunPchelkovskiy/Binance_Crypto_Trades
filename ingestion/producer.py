import json
import time
from kafka import KafkaProducer

# 1. Инициализиране на продуцента
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Адрес на вашия Kafka брокер
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Конвертиране в JSON формат
)

# Данни, които ще изпратим
data = {"user_id": 42, "action": "click", "timestamp": time.time()}

# 2. Изпращане на съобщението към темата 'user_clicks'
topic_name = 'user_clicks'
producer.send(topic_name, value=data)

# 3. Изчистване на буфера и затваряне
producer.flush()
producer.close()

print("Съобщението е изпратено успешно!")
