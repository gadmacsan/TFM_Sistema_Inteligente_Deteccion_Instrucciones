"""
schema_v1.py - Fuente unica de verdad (en codigo) del esquema Kafka `schema_v1`.

Lo usan: producer.py, el notebook del modelo y los jobs de Flink. Asi nadie
define su propia lista de columnas y todos los componentes ven los mismos campos.

Regla del proyecto: este archivo NO se edita para cambiar el esquema. Un cambio de
campos = nuevo archivo `schema_v2.py` + `schema_v2.md` (ver schema_v1.md).
La unica excepcion es corregir errores de transcripcion mientras v1 siga en borrador.
"""

import math
from datetime import datetime, timezone

SCHEMA_VERSION = "v1"
DEFAULT_TOPIC = "network-events"

# --------------------------------------------------------------------------- #
# Campos del esquema (27 columnas del dataset + 3 de metadatos = 30 en total)
# --------------------------------------------------------------------------- #
META_FIELDS = ["schema_version", "source_dataset", "timestamp_ms"]

# Tipo de cada campo de datos. int -> entero, float -> decimal.
FIELD_TYPES = {
    # identificacion de flujo
    "dst_port": int,
    "protocol": int,
    # volumen
    "flow_duration": int,
    "tot_fwd_pkts": int,
    "tot_bwd_pkts": int,
    "totlen_fwd_pkts": int,
    "totlen_bwd_pkts": int,
    # tasa
    "flow_byts_s": float,
    "flow_pkts_s": float,
    # tamano de paquete
    "fwd_pkt_len_mean": float,
    "fwd_pkt_len_std": float,
    "bwd_pkt_len_mean": float,
    "bwd_pkt_len_std": float,
    "pkt_len_mean": float,
    "pkt_len_std": float,
    # burstiness
    "flow_iat_mean": float,
    "flow_iat_std": float,
    # flags TCP
    "syn_flag_cnt": int,
    "ack_flag_cnt": int,
    "rst_flag_cnt": int,
    "fin_flag_cnt": int,
    # asimetria
    "down_up_ratio": float,
    "init_fwd_win_byts": int,
    "init_bwd_win_byts": int,
    # sesion
    "active_mean": float,
    "idle_mean": float,
}

# Features numericas = todo menos la etiqueta. Son las 26 entradas del modelo.
FEATURE_FIELDS = list(FIELD_TYPES.keys())
LABEL_FIELD = "label"
ALL_FIELDS = META_FIELDS + FEATURE_FIELDS + [LABEL_FIELD]  # 30 campos

# --------------------------------------------------------------------------- #
# Mapeo CICIDS2017 -> schema_v1
# Las claves van SIN espacios laterales: los CSV originales traen espacios
# inconsistentes (" Flow Duration", "Flow Bytes/s", ...), asi que SIEMPRE se
# compara contra el nombre ya limpiado con clean_column_name().
# --------------------------------------------------------------------------- #
TIMESTAMP_COLUMN_2017 = "Timestamp"

COLUMN_MAP_2017 = {
    "Destination Port": "dst_port",
    "Protocol": "protocol",
    "Flow Duration": "flow_duration",
    "Total Fwd Packets": "tot_fwd_pkts",
    "Total Backward Packets": "tot_bwd_pkts",
    "Total Length of Fwd Packets": "totlen_fwd_pkts",
    "Total Length of Bwd Packets": "totlen_bwd_pkts",
    "Flow Bytes/s": "flow_byts_s",
    "Flow Packets/s": "flow_pkts_s",
    "Fwd Packet Length Mean": "fwd_pkt_len_mean",
    "Fwd Packet Length Std": "fwd_pkt_len_std",
    "Bwd Packet Length Mean": "bwd_pkt_len_mean",
    "Bwd Packet Length Std": "bwd_pkt_len_std",
    "Packet Length Mean": "pkt_len_mean",
    "Packet Length Std": "pkt_len_std",
    "Flow IAT Mean": "flow_iat_mean",
    "Flow IAT Std": "flow_iat_std",
    "SYN Flag Count": "syn_flag_cnt",
    "ACK Flag Count": "ack_flag_cnt",
    "RST Flag Count": "rst_flag_cnt",
    "FIN Flag Count": "fin_flag_cnt",
    "Down/Up Ratio": "down_up_ratio",
    "Init_Win_bytes_forward": "init_fwd_win_byts",
    "Init_Win_bytes_backward": "init_bwd_win_byts",
    "Active Mean": "active_mean",
    "Idle Mean": "idle_mean",
    "Label": "label",
}


def clean_column_name(name):
    """Quita espacios laterales de una cabecera del CSV."""
    return name.strip()


# --------------------------------------------------------------------------- #
# Limpieza de valores
# --------------------------------------------------------------------------- #
_NON_NUMERIC_TOKENS = {"", "nan", "inf", "-inf", "+inf", "infinity", "-infinity", "+infinity"}


def coerce(field, raw):
    """
    Convierte un valor crudo del CSV al tipo del esquema.
    NaN, +-Infinity, vacio o ilegible -> None (null en JSON). Nunca devuelve un
    string en un campo numerico, que es lo que romperia el parseo en Flink/Spark.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if s.lower() in _NON_NUMERIC_TOKENS:
        return None
    try:
        v = float(s)
    except ValueError:
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return int(v) if FIELD_TYPES[field] is int else v


def normalize_label(raw):
    """
    Normaliza la etiqueta. 2017 usa 'BENIGN' y 2018 usa 'Benign': ambas -> 'BENIGN'.
    Tambien arregla el guion roto de 'Web Attack � Brute Force' (mojibake).
    Las etiquetas de ataque se mantienen tal cual vienen (solo se limpian espacios).
    """
    if raw is None:
        return None
    s = " ".join(str(raw).replace("�", "-").split())
    return "BENIGN" if s.lower() == "benign" else s


# --------------------------------------------------------------------------- #
# Timestamp -> epoch millis UTC
# --------------------------------------------------------------------------- #
# Formatos observados en los CSV de CICIDS2017 (no son consistentes entre dias).
TIMESTAMP_FORMATS = [
    "%d/%m/%Y %I:%M:%S %p",  # 12h con segundos y AM/PM
    "%d/%m/%Y %H:%M:%S",     # 24h con segundos
    "%d/%m/%Y %H:%M",        # 24h sin segundos
]

# Hay CSV de 2017 con horas de 12h SIN AM/PM (la tarde aparece como 1:05, 3:30...).
# La captura de CIC fue en horario laboral (09:00-17:00), asi que cualquier hora
# anterior a este umbral y sin AM/PM se interpreta como tarde (+12h).
# SUPUESTO A VALIDAR con el CSV real (ver el notebook, seccion de diagnostico).
# Poner a None para desactivar la correccion.
WORKDAY_START_HOUR = 8


def parse_timestamp_to_ms(raw, workday_start_hour=WORKDAY_START_HOUR):
    """
    Texto del CSV -> epoch en milisegundos, tratando la fecha como UTC.
    OJO: se fuerza UTC (no hora local de la maquina) para que el resultado sea
    identico en el portatil de cualquier companero y en Docker.
    """
    s = raw.strip()
    for fmt in TIMESTAMP_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
        except ValueError:
            continue
        has_ampm = "%p" in fmt
        if workday_start_hour is not None and not has_ampm and dt.hour < workday_start_hour:
            dt = dt.replace(hour=dt.hour + 12)
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    raise ValueError(f"Formato de timestamp no reconocido: {raw!r}")
