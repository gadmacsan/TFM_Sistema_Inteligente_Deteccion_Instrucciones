"""
flink_common.py - Piezas compartidas por los jobs de Flink del Hot Path.

La tabla de origen se genera a partir de schema_v1.py, asi que Flink lee EXACTAMENTE
los campos que publica el productor (una sola fuente de verdad).

Modos de tiempo (--time-mode), que deciden que columna `ts` usan las ventanas:
  ingest : hora a la que Kafka recibio el registro (monotona y robusta). POR DEFECTO.
  event  : timestamp_ms del dataset (2017). Reproduce el tiempo "real" de la captura.
  proc   : reloj de la maquina (las ventanas cierran aunque no lleguen mas eventos).
"""

import argparse

from pyflink.table import EnvironmentSettings, TableEnvironment

from schema_v1 import DEFAULT_TOPIC, FIELD_TYPES

SQL_TYPES = {int: "BIGINT", float: "DOUBLE"}


def common_args(description):
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--source", choices=["kafka", "file"], default="kafka",
                   help="'file' lee un JSONL (solo para pruebas sin Kafka)")
    p.add_argument("--file", default=None, help="Ruta del JSONL cuando --source file")
    p.add_argument("--time-mode", choices=["ingest", "event", "proc"], default="ingest")
    p.add_argument("--topic", default=DEFAULT_TOPIC)
    p.add_argument("--bootstrap", default="kafka:19092",
                   help="Dentro de Docker hay que usar kafka:19092, NO localhost:9092")
    p.add_argument("--group-id", default="flink-hotpath")
    p.add_argument("--startup", choices=["earliest-offset", "latest-offset"], default="earliest-offset")
    p.add_argument("--window-s", type=int, default=5, help="Tamano de la ventana en segundos")
    p.add_argument("--watermark-s", type=int, default=2, help="Desorden tolerado en ingest/event")
    p.add_argument("--no-wait", action="store_true",
                   help="No bloquear el cliente tras enviar el job (usar con 'flink run -d')")
    return p


def make_env(parallelism=1):
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("parallelism.default", str(parallelism))
    # UTC en toda la sesion: coincide con timestamp_ms (UTC) y evita desfases por zona horaria
    t_env.get_config().set("table.local-time-zone", "UTC")
    return t_env


def source_ddl(args):
    cols = ["schema_version STRING", "source_dataset STRING", "timestamp_ms BIGINT"]
    cols += [f"{name} {SQL_TYPES[t]}" for name, t in FIELD_TYPES.items()]
    cols += ["label STRING"]

    watermark = None
    if args.time_mode == "ingest":
        if args.source != "kafka":
            raise SystemExit("--time-mode ingest necesita --source kafka (usa el timestamp del registro)")
        cols.append("ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'")
        watermark = f"WATERMARK FOR ts AS ts - INTERVAL '{args.watermark_s}' SECOND"
    elif args.time_mode == "event":
        cols.append("ts AS TO_TIMESTAMP_LTZ(timestamp_ms, 3)")
        watermark = f"WATERMARK FOR ts AS ts - INTERVAL '{args.watermark_s}' SECOND"
    else:
        cols.append("ts AS PROCTIME()")

    if watermark:
        cols.append(watermark)

    if args.source == "kafka":
        with_clause = {
            "connector": "kafka",
            "topic": args.topic,
            "properties.bootstrap.servers": args.bootstrap,
            "properties.group.id": args.group_id,
            "scan.startup.mode": args.startup,
            "format": "json",
            "json.ignore-parse-errors": "true",
        }
    else:
        if not args.file:
            raise SystemExit("--source file requiere --file")
        with_clause = {"connector": "filesystem", "path": args.file, "format": "json"}

    with_sql = ",\n  ".join(f"'{k}' = '{v}'" for k, v in with_clause.items())
    return "CREATE TABLE network_events (\n  " + ",\n  ".join(cols) + f"\n) WITH (\n  {with_sql}\n)"


def create_source(t_env, args):
    t_env.execute_sql(source_ddl(args))


def run(statement_set, args):
    result = statement_set.execute()
    if not args.no_wait:
        result.wait()
