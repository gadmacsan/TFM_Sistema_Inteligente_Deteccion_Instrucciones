# TFM A — Hot Path: simulador de streaming CICIDS2017

Mini demo del productor/consumidor Kafka que reproduce el dataset CICIDS2017 (Wednesday-workingHours, DoS/Heartbleed) como si fuera trafico en tiempo real, respetando el orden y el ritmo temporal original de captura.

Este es el primer eslabon del pipeline completo (Kafka -> Flink -> clasificador ML -> Grafana). Este repositorio cubre solo la parte de ingesta simulada; Flink y el modelo se anadiran en las siguientes fases.

## Requisitos

- Docker Desktop
- Python 3.10 o superior
- El CSV `Wednesday-workingHours.pcap_ISCX.csv` de CICIDS2017, con las columnas `Timestamp` y `Protocol` incluidas (no incluido en el repo, ver abajo)

## Descargar el dataset

El CSV no se sube al repositorio por su tamano y por los terminos de uso del dataset.

Descargalo de la fuente oficial del CIC (https://www.unb.ca/cic/datasets/ids-2017.html), concretamente el archivo **`GeneratedLabelledFlows.zip`**, no `MachineLearningCSV.zip`. Este segundo recorta las columnas `Timestamp` y `Protocol`, que el productor necesita para reproducir el ritmo temporal real. Dentro del zip esta `Wednesday-workingHours.pcap_ISCX.csv` (puede venir dentro de una subcarpeta tipo `TrafficLabelling/`).

Coloca el CSV dentro de una carpeta `data/` en la raiz del proyecto (no se versiona, ya esta en el `.gitignore`).

## Instalacion

```bash
git clone <url-del-repo>
cd tfm-hotpath-demo

python -m venv venv
venv\Scripts\activate      # Windows
# source venv/bin/activate   # macOS/Linux

pip install -r requirements.txt
```

> Nota: usamos `kafka-python-ng` en vez de `kafka-python` porque este ultimo falla con `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'` en Python 3.12 o superior. `kafka-python-ng` es un fork compatible que se importa igual (`from kafka import KafkaProducer`).

## Levantar Kafka local

```bash
docker compose up -d
```

Esto arranca un unico broker de Kafka (imagen oficial `apache/kafka`) en modo KRaft (sin Zookeeper), escuchando en `localhost:9092`.

## Ejecutar la demo

El bloque inicial del CSV es trafico benigno de fondo (DNS, NetBIOS, NTP); los ataques DoS aparecen mas adelante en el fichero. Usa `--skip` para saltarte el tramo tranquilo:

En una terminal (con el entorno virtual activado):

```bash
python producer.py --csv data/Wednesday-workingHours.pcap_ISCX.csv --skip 300000 --limit 2000
```

Si el resumen final muestra 0 ataques, sube el valor de `--skip` (prueba 400000, luego 500000) hasta encontrar la zona de ataques.

En otra terminal, en paralelo:

```bash
python consumer.py
```

El consumidor muestra en vivo el numero de eventos recibidos y cuantos son ataques, segun van llegando por Kafka.

## Estructura del proyecto

```
.
├── docker-compose.yml   # Kafka local (apache/kafka) en modo KRaft
├── producer.py          # Reproduce el CSV como stream hacia Kafka
├── consumer.py          # Consumidor de demo con contador en vivo
├── requirements.txt
├── .gitignore
├── README.md
└── data/                # CSV del dataset (no versionado, crear localmente)
```

## Parametros del productor

| Parametro | Por defecto | Descripcion |
|---|---|---|
| `--csv` | (obligatorio) | Ruta al CSV |
| `--topic` | `network-events` | Topic de Kafka |
| `--bootstrap-servers` | `localhost:9092` | Direccion del broker |
| `--speed` | `50` | Multiplicador de velocidad respecto al ritmo real de captura (solo aplica si el CSV trae Timestamp real) |
| `--limit` | `2000` | Numero maximo de filas a reproducir |
| `--skip` | `0` | Numero de filas a saltar antes de empezar (para llegar a la zona de ataques) |
| `--max-gap-s` | `3.0` | Tope de espera entre eventos consecutivos |
| `--row-interval-ms` | `80` | Espera fija entre eventos cuando el CSV no trae Timestamp real (modo sintetico) |

## Parametros del consumidor

| Parametro | Por defecto | Descripcion |
|---|---|---|
| `--topic` | `network-events` | Topic de Kafka |
| `--bootstrap-servers` | `localhost:9092` | Direccion del broker |
