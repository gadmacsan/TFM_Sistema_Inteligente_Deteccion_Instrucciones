"""
consumer.py - Consumidor minimo para la demo: muestra en vivo los eventos que llegan
al topic y un contador acumulado de benignos vs ataques.

Sirve para dos topics:
  - network-events : cuenta segun la etiqueta real (`label`)
  - predictions    : cuenta segun la etiqueta que predice el modelo (`predicted_label`)

Uso:
    pip install -r requirements.txt
    python consumer.py
    python consumer.py --topic predictions
"""

import json
import argparse

from kafka import KafkaConsumer

from schema_v1 import DEFAULT_TOPIC

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
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
            label = event.get("label") or event.get("predicted_label")
            if label != "BENIGN":
                ataques += 1
            print(f"\rEventos recibidos: {total} | Ataques: {ataques}", end="", flush=True)
    except KeyboardInterrupt:
        print("\nDetenido.")
