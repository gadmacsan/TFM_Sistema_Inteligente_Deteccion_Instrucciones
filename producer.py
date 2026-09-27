"""
producer.py - Simula trafico "en tiempo real" reproduciendo un CSV de CICIDS2017
hacia un topic de Kafka.

Este mirror de Kaggle (chethuhn/network-intrusion-dataset) no incluye las columnas
Timestamp ni Protocol. Cuando faltan, el script genera un timestamp sintetico
respetando el orden original de las filas del CSV, y asume protocolo TCP (6) por
defecto. Si en el futuro usais un CSV que si trae esas columnas (p. ej. el oficial
de unb.ca/cic), el script las detecta automaticamente y usa los valores reales.

Uso:
    pip install kafka-python-ng
    python producer.py --csv data/Wednesday-workingHours.pcap_ISCX.csv --limit 2000
    python producer.py --csv data/Wednesday-workingHours.pcap_ISCX.csv --skip 150000 --limit 500
"""

import csv
import json
import time
import argparse
from datetime import datetime

from kafka import KafkaProducer

TIMESTAMP_COLUMN_CANDIDATES = ["Timestamp", " Timestamp"]
PROTOCOL_COLUMN_CANDIDATES = ["Protocol", " Protocol"]

COLUMN_MAP_2017 = {
    " Destination Port": "dst_port",
    " Flow Duration": "flow_duration",
    " Total Fwd Packets": "tot_fwd_pkts",
    " Total Backward Packets": "tot_bwd_pkts",
    "Total Length of Fwd Packets": "totlen_fwd_pkts",
    " Total Length of Bwd Packets": "totlen_bwd_pkts",
    "Flow Bytes/s": "flow_byts_s",
    " Flow Packets/s": "flow_pkts_s",
    " Fwd Packet Length Mean": "fwd_pkt_len_mean",
    " Fwd Packet Length Std": "fwd_pkt_len_std",
    " Bwd Packet Length Mean": "bwd_pkt_len_mean",
    " Bwd Packet Length Std": "bwd_pkt_len_std",
    " Packet Length Mean": "pkt_len_mean",
    " Packet Length Std": "pkt_len_std",
    "Flow IAT Mean": "flow_iat_mean",
    " Flow IAT Std": "flow_iat_std",
    " SYN Flag Count": "syn_flag_cnt",
    " ACK Flag Count": "ack_flag_cnt",
    " RST Flag Count": "rst_flag_cnt",
    "FIN Flag Count": "fin_flag_cnt",
    " Down/Up Ratio": "down_up_ratio",
    "Init_Win_bytes_forward": "init_fwd_win_byts",
    " Init_Win_bytes_backward": "init_bwd_win_byts",
    "Active Mean": "active_mean",
    "Idle Mean": "idle_mean",
    " Label": "label",
}

TIMESTAMP_FORMATS = ["%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"]


def parse_timestamp_to_ms(raw):
    raw = raw.strip()
    for fmt in TIMESTAMP_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(f"Formato de timestamp no reconocido: {raw!r}")


def to_number(value):
    try:
        if value is None or value == "":
            return None
        if isinstance(value, str) and ("." in value or "e" in value.lower()):
            return float(value)
        return int(value)
    except (ValueError, TypeError, AttributeError):
        return value


def load_events(csv_path, source_dataset, column_map, limit, skip, row_interval_ms):
    events = []
    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

        ts_col = next((c for c in TIMESTAMP_COLUMN_CANDIDATES if c in fieldnames), None)
        proto_col = next((c for c in PROTOCOL_COLUMN_CANDIDATES if c in fieldnames), None)

        if ts_col is None:
            print("Aviso: este CSV no trae columna de Timestamp (mirror recortado de Kaggle).")
            print("       Se genera un timestamp sintetico respetando el orden original de las filas.\n")
        if proto_col is None:
            print("Aviso: este CSV no trae columna de Protocol. Se asume 6 (TCP) por defecto.\n")

        synthetic_base_ms = int(time.time() * 1000)

        for i, row in enumerate(reader):
            if i < skip:
                continue
            if limit and (i - skip) >= limit:
                break

            event = {}
            for original_col, target_field in column_map.items():
                if original_col in row:
                    event[target_field] = row[original_col]

            if ts_col:
                event["timestamp_ms"] = parse_timestamp_to_ms(row[ts_col])
            else:
                event["timestamp_ms"] = synthetic_base_ms + i * row_interval_ms

            event["protocol"] = to_number(row[proto_col]) if proto_col else 6

            for field, value in list(event.items()):
                if field != "label" and not isinstance(value, (int, float)):
                    event[field] = to_number(value)

            event["schema_version"] = "v1"
            event["source_dataset"] = source_dataset
            events.append(event)

    return events, (ts_col is not None)


def replay(events, topic, bootstrap_servers, speed, max_gap_s, has_real_timestamps, row_interval_ms):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    modo = f"real, acelerado x{speed}" if has_real_timestamps else f"sintetico ({row_interval_ms} ms/evento)"
    print(f"Reproduciendo {len(events)} eventos en modo {modo}...\n")

    prev_ts = None
    benignos = 0
    ataques = 0

    for event in events:
        if has_real_timestamps and prev_ts is not None:
            delta_s = (event["timestamp_ms"] - prev_ts) / 1000
            sleep_s = min(max(0.0, delta_s / speed), max_gap_s)
        elif not has_real_timestamps:
            sleep_s = row_interval_ms / 1000
        else:
            sleep_s = 0
        time.sleep(sleep_s)
        prev_ts = event["timestamp_ms"]

        producer.send(topic, event)

        label = event.get("label", "?")
        if label == "BENIGN":
            benignos += 1
            flag = "verde  (benigno)"
        else:
            ataques += 1
            flag = "ROJO   (ATAQUE)"

        print(f"{flag} | {str(label):<20} | puerto {event.get('dst_port')}")

    producer.flush()
    print(f"\nReproduccion terminada. Total: {len(events)} | Benignos: {benignos} | Ataques: {ataques}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Ruta al CSV, p. ej. Wednesday-workingHours.pcap_ISCX.csv")
    parser.add_argument("--topic", default="network-events")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--speed", type=float, default=50.0, help="Multiplicador de velocidad (solo si el CSV trae Timestamp real)")
    parser.add_argument("--limit", type=int, default=2000, help="Numero maximo de filas a reproducir")
    parser.add_argument("--skip", type=int, default=0, help="Numero de filas a saltar antes de empezar (para llegar a la zona de ataques)")
    parser.add_argument("--max-gap-s", type=float, default=3.0, help="Tope de espera entre eventos cuando hay timestamp real")
    parser.add_argument("--row-interval-ms", type=int, default=80, help="Espera fija entre eventos cuando no hay timestamp real (modo sintetico)")
    args = parser.parse_args()

    events, has_real_ts = load_events(args.csv, "cicids2017", COLUMN_MAP_2017, args.limit, args.skip, args.row_interval_ms)
    replay(events, args.topic, args.bootstrap_servers, args.speed, args.max_gap_s, has_real_ts, args.row_interval_ms)
