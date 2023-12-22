from confluent_kafka import Consumer, KafkaException
import json
import pandas as pd
import time
from tabulate import tabulate

TOPIC_NAME = "some_topic"
TIME_DELAY = 20

def run_consumers():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "group1",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([TOPIC_NAME])

    try:
        list_of_jsons = []
        start = time.time()
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            payload = msg.value().decode("utf-8")
            data = json.loads(payload)
            list_of_jsons.append(data)
            if time.time() - start > TIME_DELAY:
                print(f"[INFO] passed {TIME_DELAY} seconds")
                df = pd.DataFrame(list_of_jsons)
                print(f"MEAN TYPE")
                df1 = df.groupby("type").value.mean()
                print(df1.to_markdown())
                print(f"MEAN NAME")
                df2 = df.groupby("name").value.mean()
                print(df2.to_markdown())
                start = time.time()
                print("-"*100)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumers()