"""
consumer.py - Consumidor minimo para la demo: muestra en vivo los eventos que llegan
al topic y un contador acumulado de benignos vs ataques.

Uso:
    pip install kafka-python
    python consumer.py
"""

import json
import argparse

from kafka import KafkaConsumer

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="network-events")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    total = 0
    ataques = 0

    print("Esperando eventos... (Ctrl+C para salir)\n")
    try:
        for msg in consumer:
            event = msg.value
            total += 1
            if event.get("label") != "BENIGN":
                ataques += 1
            print(f"\rEventos recibidos: {total} | Ataques: {ataques}", end="", flush=True)
    except KeyboardInterrupt:
        print("\nDetenido.")
