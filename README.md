# TFM A — Hot Path: detección de intrusiones en streaming

Pipeline del Hot Path de la arquitectura Kappa (Kafka como fuente única de verdad):

```
CSV CICIDS2017  ->  producer.py  ->  Kafka (network-events)  ->  Flink  ->  modelo ONNX  ->  Kafka (predictions)  ->  Grafana
                    (replay)                                       (ventanas + UDF)
```


| Fase | Estado | Dónde |
|---|---|---|
| 1. Ingesta simulada (replay del CSV a Kafka) | Hecho | `producer.py`, `consumer.py` |
| 2. Esquema unificado compartido con el Cold Path | Hecho (borrador v1) | `schema_v1.md`, `schema_v1.py` |
| 3. Modelo clasificador (entrenamiento offline) | Hecho: entrenado y evaluado con el CSV real del miércoles (F1 macro 0,90 en prueba; ver "Modelo") | `notebooks/01_modelo_hotpath.ipynb` |
| 4. Flink: ventanas y inferencia | Probado en local con datos sintéticos; pendiente de verificar con Docker y Kafka reales | `flink/` |
| 5. Sink a base de datos + Grafana | Pendiente | |

## Requisitos

- Docker Desktop
- Python 3.10 o superior
- El CSV `Wednesday-workingHours.pcap_ISCX.csv` de CICIDS2017, con las columnas `Timestamp` y `Protocol` (no está en el repo, ver abajo)

## Descargar el dataset

El CSV no se sube al repositorio por su tamaño y por los términos de uso del dataset.

Descárgalo de la fuente oficial del CIC (https://www.unb.ca/cic/datasets/ids-2017.html), concretamente el archivo **`GeneratedLabelledFlows.zip`**, no `MachineLearningCSV.zip`. Este segundo recorta las columnas `Timestamp` y `Protocol`, que el productor necesita para reproducir el ritmo temporal real. Dentro del zip está `Wednesday-workingHours.pcap_ISCX.csv` (puede venir dentro de una subcarpeta tipo `TrafficLabelling/`).

Coloca el CSV dentro de una carpeta `data/` en la raíz del proyecto (no se versiona, ya está en el `.gitignore`).

## Instalación

```bash
git clone <url-del-repo>
cd <carpeta-del-repo>

python -m venv venv
venv\Scripts\activate      # Windows
# source venv/bin/activate   # macOS/Linux

pip install -r requirements.txt            # productor y consumidor
pip install -r requirements-notebook.txt   # solo si vas a ejecutar el notebook del modelo
```

> Usamos `kafka-python-ng` en vez de `kafka-python` porque este último falla con `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'` en Python 3.12 o superior. `kafka-python-ng` es un fork compatible que se importa igual (`from kafka import KafkaProducer`).

## Levantar Kafka local

```bash
docker compose up -d
```

Arranca un único broker de Kafka (imagen oficial `apache/kafka`) en modo KRaft (sin Zookeeper). Espera unos 15 segundos y comprueba con `docker compose ps` que `kafka-demo` está en *running* antes de lanzar el productor. Dos direcciones según desde dónde te conectes:

| Desde dónde | Dirección |
|---|---|
| Tu máquina (scripts de Python) | `localhost:9092` |
| Otro contenedor (Flink) | `kafka:19092` |

Kafka no guarda datos en disco: `docker compose down` borra los topics, lo que sirve para empezar de cero.

Si `docker compose up` dice que el nombre `/kafka-demo` ya está en uso (queda un contenedor de una versión anterior), ejecuta `docker rm -f kafka-demo` y repite.

## Ejecutar la demo de replay

El CSV no está en orden de ataques: unas zonas son casi todo tráfico benigno y otras casi todo ataque. **No adivines el `--skip`**: el productor tiene un modo `--scan` que recorre el CSV (sin Kafka) y muestra en qué filas hay ataques.

```bash
python producer.py --csv data/Wednesday-workingHours.pcap_ISCX.csv --scan
```

La salida trae una tabla por tramos de filas, la primera y la última fila de cada etiqueta y una línea final con el `--skip` sugerido (el primer tramo con al menos un 20 % de ataques). `--skip` cuenta filas en el **orden del fichero**, no en orden de hora.

En una terminal (con el entorno virtual activado), sustituyendo `<N>` por el valor sugerido:

```bash
python producer.py --csv data/Wednesday-workingHours.pcap_ISCX.csv --skip <N> --limit 2000
```

Si el resumen final muestra 0 ataques, repite el `--scan` y elige otro tramo. En otra terminal, en paralelo:

```bash
python consumer.py
```

El consumidor muestra en vivo cuántos eventos llegan y cuántos son ataques. Como lee desde el principio del topic, el contador **incluye también los mensajes de ejecuciones anteriores**; si quieres empezar de cero, `docker compose down` y vuelve a levantar. Lanza los comandos de uno en uno: si el productor arranca antes de que Kafka esté listo, falla con `NoBrokersAvailable`.

## Modelo (notebook)

`notebooks/01_modelo_hotpath.ipynb` carga el CSV, lo mapea a `schema_v1`, limpia, hace un **split cronológico por clase con embargo**, entrena un Random Forest, evalúa (matriz de confusión, precision/recall/F1, vista binaria), mide la latencia y exporta:

| Fichero (en `models/`, no se versiona) | Para qué |
|---|---|
| `hotpath_rf_v1.onnx` | El modelo que usa Flink |
| `hotpath_rf_v1.json` | Orden de features, clases, métricas y versiones |
| `hotpath_rf_v1.joblib` | Reutilizar el modelo en el notebook |

Las figuras para la memoria quedan en `reports/`. Se ejecuta con `jupyter lab` desde la raíz del repo, o con `python -m jupyter lab` desde el venv para que el kernel sea el del entorno donde instalaste los paquetes.

Si falla un `import` con `ModuleNotFoundError`, el kernel no es el del venv. Instala en el kernel activo con `%pip install -r ../requirements-notebook.txt` en una celda y reinicia el kernel.

**Partición de los datos.** Cada clase se ordena por tiempo y se corta por separado en bloques con el patrón entrenamiento, entrenamiento, entrenamiento, validación, prueba (60/20/20), con un embargo de 60 s (el CSV solo tiene resolución de 1 minuto). Una clase usa 20 bloques si cada uno dura al menos 5 minutos y 5 si no, y las clases con menos de 500 filas van enteras a entrenamiento. Un corte global por bloques de igual número de filas dejaba el test sin ataques, porque cada ataque es un único episodio corto y muy denso.

**Resultados con el CSV del miércoles** (114 866 flujos de prueba; los detalles están en el notebook y en `models/hotpath_rf_v1.json`):

| Medida | Valor |
|---|---|
| F1 macro (5 clases con soporte) | 0,90 |
| Vista binaria: exhaustividad / precisión / falsos positivos | 0,955 / 0,998 / 0,07 % |
| Latencia por evento, mediana: scikit-learn / ONNX | 8,8 ms / 0,03 ms |

Hay que leerlos con cautela: la prueba procede de los mismos episodios de ataque que el entrenamiento, así que son optimistas, y Slowhttptest tiene solo 76 filas de prueba. La generalización real se mide con un día de CSE-CIC-IDS2018. El error más relevante es que un 4,4 % de Hulk se clasifica como benigno. `OTHER_ATTACK` (Heartbleed, 11 filas) no se puede evaluar.

## Flink

Ver [`flink/README.md`](flink/README.md). Necesita `models/hotpath_rf_v1.onnx` y `.json`, que genera el notebook. Resumen:

```bash
docker compose --profile flink up -d --build        # la primera vez tarda varios minutos
docker compose exec jobmanager flink run -d -py /opt/job/jobs/01_ventanas_sin_ml.py --no-wait   # trabajo 1: ventanas sin modelo
docker compose exec jobmanager flink list           # para ver el JOB_ID
docker compose exec jobmanager flink cancel <JOB_ID>
docker compose exec jobmanager flink run -d -py /opt/job/jobs/02_inferencia_onnx.py --no-wait   # trabajo 2: inferencia con ONNX
python consumer.py --topic predictions              # lee las predicciones
```

El orden recomendado es: primero Kafka solo, luego Flink sin modelo, luego Flink con modelo. La interfaz web está en http://localhost:8081. En Git Bash para Windows, antepón `MSYS_NO_PATHCONV=1` a los `docker compose exec`, o las rutas `/opt/...` se transforman.

> Estado: la imagen de Flink, la versión del conector de Kafka (`KAFKA_CONNECTOR_VERSION` en `flink/Dockerfile`) y `flink run ... --no-wait` **no están verificados** con Docker real. Si el build falla con un `404` al descargar el conector, ajusta esa versión (`flink-sql-connector-kafka`, versión que acabe en `-1.20`).

## Solución de problemas

| Síntoma | Causa y solución |
|---|---|
| `NoBrokersAvailable` | Kafka no está arrancado o el contenedor chocó de nombre. `docker compose ps`; si hace falta, `docker rm -f kafka-demo` y `docker compose up -d` |
| El productor reproduce solo `BENIGN` | El `--skip` cae en una zona benigna. Ejecuta `--scan` y usa el valor sugerido |
| `ModuleNotFoundError` en el notebook | Kernel equivocado. Ver la sección "Modelo" |
| El notebook para con "el test no tiene ningun ataque" | La partición no deja ataques en prueba. Revisa la tabla de la sección 4 del notebook |
| El trabajo 2 falla con el orden de features | El `.onnx` no coincide con `schema_v1.py`. Repite el notebook |
| Al relanzar un trabajo aparecen eventos viejos | Lee desde `earliest-offset`. `docker compose --profile flink down` para empezar limpio |

## Estructura del proyecto

```
.
├── docker-compose.yml          # Kafka siempre; Flink con --profile flink
├── schema_v1.py                # Esquema en código: campos, tipos, mapeo y limpieza (fuente única)
├── schema_v1.md                # Esquema explicado
├── producer.py                 # Reproduce el CSV como stream hacia Kafka
├── consumer.py                 # Consumidor de demo con contador en vivo
├── notebooks/
│   └── 01_modelo_hotpath.ipynb # Entrenamiento y evaluación del modelo
├── flink/
│   ├── Dockerfile
│   ├── README.md
│   └── jobs/                   # flink_common.py, 01_ventanas_sin_ml.py, 02_inferencia_onnx.py
├── models/                     # Modelos generados (no versionados)
├── reports/                    # Figuras del notebook
├── requirements.txt
├── requirements-notebook.txt
├── .gitignore
├── README.md
└── data/                       # CSV del dataset (no versionado, crear localmente)
```

## Parámetros del productor

| Parámetro | Por defecto | Descripción |
|---|---|---|
| `--csv` | (obligatorio) | Ruta al CSV |
| `--topic` | `network-events` | Topic de Kafka |
| `--bootstrap-servers` | `localhost:9092` | Dirección del broker |
| `--speed` | `50` | Multiplicador de velocidad respecto al ritmo real de captura (solo con Timestamp real) |
| `--limit` | `2000` | Número máximo de filas a reproducir |
| `--skip` | `0` | Filas a saltar antes de empezar (para llegar a la zona de ataques) |
| `--max-gap-s` | `3.0` | Tope de espera entre eventos consecutivos |
| `--row-interval-ms` | `80` | Espera fija entre eventos si el CSV no trae Timestamp (modo sintético) |
| `--no-hour-fix` | desactivado | Desactiva la corrección +12 h de horas de tarde sin AM/PM (ver `schema_v1.md`) |
| `--scan` | desactivado | No reproduce nada: muestra en qué filas del CSV hay ataques y sugiere un `--skip` |
| `--scan-chunk` | `25000` | Tamaño de cada tramo de la tabla de `--scan` |

Detalles que conviene saber: las filas elegidas se **ordenan por `timestamp_ms`** antes de reproducirlas, `NaN`/`Infinity` se envían como `null`, y el timestamp se interpreta siempre como **UTC** (no como hora local).

## Parámetros del consumidor

| Parámetro | Por defecto | Descripción |
|---|---|---|
| `--topic` | `network-events` | Topic de Kafka (`predictions` para ver la salida del modelo) |
| `--bootstrap-servers` | `localhost:9092` | Dirección del broker |
