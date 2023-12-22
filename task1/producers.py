from confluent_kafka import Producer, admin
import time
import random
import numpy as np
import json

TOPIC_NAME = "some_topic"

K_PRODUCERS = 6
TYPES = [("temperature", (10, 30)),
         ("pressure", (760, 50)),
         ("wind_direction", (180, 60)), 
         ("wind_speed", (5, 5)),
         ("humidity", (70, 20)),
         ("cloudiness", (0, 1))]

names = []
list_producers = []

def delivery_report(err, msg):
    if err is not None:
        print(f'[ERROR] Ошибка доставки сообщения: {err}')
    else:
        print(f'[INFO] Сообщение успешно доставлено: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}')


def run_produces():
    for i in range(K_PRODUCERS):
        producer = Producer({
                "bootstrap.servers": "localhost:9092"
            })
        time_delay = random.randint(0, 3)
        list_producers.append((i+1, producer, time_delay))
        
    while True:
        # time.sleep(1)
        num, producer, time_delay = random.choice(list_producers)
        type, (mean, std) = random.choice(TYPES)
        # время события, "тип" датчика (температура, давление, и т.п.), имя датчика, значение (число с плавающей точкой).
            # время события, "тип" датчика (температура, давление, и т.п.), имя датчика, значение (число с плавающей точкой).
        data = {
                    "timestamp": time.time(),
                    "type": type,
                    "name": f"Device {num}",
                    "value": np.random.normal(mean, std)
                }


        producer.produce(TOPIC_NAME, key=str(time.time()).encode('utf-8'), value=json.dumps(data, indent=2).encode('utf-8'),
                         callback=delivery_report)
        time.sleep(time_delay)
        producer.flush()


def delete_topic(topic_name):
    a = admin.AdminClient({
                "bootstrap.servers": "localhost:9092"
            })
    result = a.delete_topics([topic_name])
    for topic, future in result.items():
        try:
            future.result()  # Дождитесь завершения удаления
            print(f"Тема {topic} успешно удалена")
        except Exception as e:
            print(f"Ошибка при удалении темы {topic}: {str(e)}")

if __name__ == "__main__":
    run_produces()
    # delete_topic(TOPIC_NAME)