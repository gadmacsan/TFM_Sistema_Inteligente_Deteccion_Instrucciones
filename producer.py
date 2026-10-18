"""
producer.py - Reproduce un CSV de CICIDS2017 como trafico "en tiempo real" hacia Kafka.

Requiere el CSV de GeneratedLabelledFlows.zip (trae Timestamp y Protocol). Si faltan,
genera un timestamp sintetico y asume protocolo TCP (6).

Uso:
    pip install -r requirements.txt
    python producer.py --csv data/Wednesday-workingHours.pcap_ISCX.csv --scan          # 1) localizar los ataques
    python producer.py --csv data/Wednesday-workingHours.pcap_ISCX.csv --skip N --limit 2000   # 2) reproducir desde N

Notas de diseno:
  - El nombre de columna se compara SIN espacios laterales (ver schema_v1.py).
  - NaN / Infinity se envian como null, nunca como string.
  - timestamp_ms es epoch UTC. El Kafka record timestamp (hora de envio) se usa
    aparte para medir latencia extremo a extremo; no hace falta un campo extra.
  - Las filas elegidas por --skip/--limit se ordenan por timestamp_ms antes de
    reproducirlas, porque el CSV no esta estrictamente en orden cronologico.
"""

import argparse
import csv
import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from schema_v1 import (
    COLUMN_MAP_2017,
    DEFAULT_TOPIC,
    SCHEMA_VERSION,
    TIMESTAMP_COLUMN_2017,
    WORKDAY_START_HOUR,
    clean_column_name,
    coerce,
    normalize_label,
    parse_timestamp_to_ms,
)


def load_events(csv_path, source_dataset, limit, skip, row_interval_ms, workday_start_hour):
    events = []
    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = [clean_column_name(c) for c in next(reader)]
        index = {name: i for i, name in enumerate(header)}

        ts_idx = index.get(TIMESTAMP_COLUMN_2017)
        has_real_ts = ts_idx is not None

        # columnas del esquema que no aparecen en este CSV (el protocolo se trata aparte)
        missing = [c for c in COLUMN_MAP_2017 if c not in index and c != "Protocol"]
        if missing:
            print(f"AVISO: columnas del esquema que no estan en el CSV (iran como null): {missing}\n")
        if not has_real_ts:
            print("AVISO: el CSV no trae Timestamp. Se genera uno sintetico respetando el orden de filas.")
            print("       Usa GeneratedLabelledFlows.zip para tener el tiempo real.\n")
        if "Protocol" not in index:
            print("AVISO: el CSV no trae Protocol. Se asume 6 (TCP).\n")

        synthetic_base_ms = int(time.time() * 1000)

        for i, row in enumerate(reader):
            if i < skip:
                continue
            if limit and (i - skip) >= limit:
                break
            if len(row) < len(header):  # fila truncada
                continue

            event = {"schema_version": SCHEMA_VERSION, "source_dataset": source_dataset}

            if has_real_ts:
                event["timestamp_ms"] = parse_timestamp_to_ms(row[ts_idx], workday_start_hour)
            else:
                event["timestamp_ms"] = synthetic_base_ms + i * row_interval_ms

            for col, field in COLUMN_MAP_2017.items():
                if field == "label":
                    continue
                if col in index:
                    event[field] = coerce(field, row[index[col]])
                elif field == "protocol":
                    event[field] = 6
                else:
                    event[field] = None

            event["label"] = normalize_label(row[index["Label"]]) if "Label" in index else None
            events.append(event)

    if has_real_ts:
        events.sort(key=lambda e: e["timestamp_ms"])  # sort estable
    return events, has_real_ts


def scan_labels(csv_path, chunk):
    """Recorre el CSV (sin Kafka) y muestra DONDE estan los ataques en el orden del fichero.
    --skip cuenta filas en ese orden, asi que esta tabla dice que valor usar."""
    from collections import Counter, OrderedDict
    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = [clean_column_name(c) for c in next(reader)]
        index = {name: i for i, name in enumerate(header)}
        if "Label" not in index:
            raise SystemExit("El CSV no tiene columna Label")
        li = index["Label"]
        total, chunks, first, last, cur = 0, [], OrderedDict(), {}, Counter()
        for i, row in enumerate(reader):
            if len(row) <= li:
                continue
            lab = normalize_label(row[li])
            cur[lab] += 1
            first.setdefault(lab, i); last[lab] = i
            total = i + 1
            if total % chunk == 0:
                chunks.append((total - chunk, total - 1, cur)); cur = Counter()
        if cur:
            chunks.append((total - sum(cur.values()), total - 1, cur))
    print(f"Filas totales: {total}\n")
    print(f"{'filas (orden del fichero)':<28}{'benignos':>10}{'ataques':>10}  tipos de ataque")
    for a, b, c in chunks:
        att = {k: v for k, v in c.items() if k != "BENIGN"}
        det = ", ".join(f"{k} {v}" for k, v in sorted(att.items(), key=lambda kv: -kv[1]))
        print(f"{a:>10} - {b:<15}{c.get('BENIGN', 0):>10}{sum(att.values()):>10}  {det}")
    print("\nPrimera / ultima fila de cada etiqueta:")
    for lab, i in first.items():
        print(f"  {lab:<20} {i:>9} -> {last[lab]:>9}")
    dense = [(a, c) for a, b, c in chunks if sum(v for k, v in c.items() if k != "BENIGN") >= 0.2 * sum(c.values())]
    if dense:
        a, c = dense[0]
        print(f"\nSugerencia: --skip {a}  (primer tramo con >= 20 % de ataques; mezcla benigno y ataque)")
    else:
        print("\nNo hay ningun tramo con >= 20 % de ataques; usa --scan-chunk menor o revisa el CSV.")


def fmt_time(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%H:%M:%S")


def replay(events, topic, bootstrap_servers, speed, max_gap_s, has_real_ts, row_interval_ms):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    modo = f"real, acelerado x{speed:g}" if has_real_ts else f"sintetico ({row_interval_ms} ms/evento)"
    print(f"Reproduciendo {len(events)} eventos en modo {modo}...\n")

    prev_ts = None
    benignos = ataques = 0

    for event in events:
        if has_real_ts:
            if prev_ts is None:
                sleep_s = 0.0
            else:
                delta_s = (event["timestamp_ms"] - prev_ts) / 1000
                sleep_s = min(max(0.0, delta_s / speed), max_gap_s)
        else:
            sleep_s = row_interval_ms / 1000
        time.sleep(sleep_s)
        prev_ts = event["timestamp_ms"]

        producer.send(topic, event)

        label = event.get("label") or "?"
        if label == "BENIGN":
            benignos += 1
            flag = "verde  (benigno)"
        else:
            ataques += 1
            flag = "ROJO   (ATAQUE)"
        print(f"[{fmt_time(event['timestamp_ms'])}] {flag} | {label:<20} | puerto {event.get('dst_port')}")

    producer.flush()
    print(f"\nReproduccion terminada. Total: {len(events)} | Benignos: {benignos} | Ataques: {ataques}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Ruta al CSV, p. ej. data/Wednesday-workingHours.pcap_ISCX.csv")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--speed", type=float, default=50.0, help="Multiplicador de velocidad (solo con Timestamp real)")
    parser.add_argument("--limit", type=int, default=2000, help="Numero maximo de filas a reproducir")
    parser.add_argument("--skip", type=int, default=0, help="Filas a saltar antes de empezar (para llegar a la zona de ataques)")
    parser.add_argument("--max-gap-s", type=float, default=3.0, help="Tope de espera entre eventos con timestamp real")
    parser.add_argument("--row-interval-ms", type=int, default=80, help="Espera fija entre eventos en modo sintetico")
    parser.add_argument("--no-hour-fix", action="store_true",
                        help=f"Desactiva la correccion +12h de horas sin AM/PM anteriores a las {WORKDAY_START_HOUR}:00")
    parser.add_argument("--scan", action="store_true",
                        help="No reproduce nada: muestra en que filas del CSV hay ataques y sugiere un --skip")
    parser.add_argument("--scan-chunk", type=int, default=25000, help="Tamano del tramo de la tabla de --scan")
    args = parser.parse_args()

    if args.scan:
        scan_labels(args.csv, args.scan_chunk)
        raise SystemExit(0)

    workday_hour = None if args.no_hour_fix else WORKDAY_START_HOUR
    events, has_real_ts = load_events(
        args.csv, "cicids2017", args.limit, args.skip, args.row_interval_ms, workday_hour
    )
    replay(events, args.topic, args.bootstrap_servers, args.speed, args.max_gap_s, has_real_ts, args.row_interval_ms)
