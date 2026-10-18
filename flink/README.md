# Flink: Hot Path en streaming

> **Frase para memorizar:** *Kafka guarda, Flink calcula, ONNX decide, Grafana enseña.*

## Qué hay aquí

| Fichero | Qué hace |
|---|---|
| `Dockerfile` | Flink 1.20 + PyFlink + conector Kafka + `onnxruntime` |
| `jobs/flink_common.py` | Tabla de origen **generada desde `schema_v1.py`**, argumentos y zona horaria UTC |
| `jobs/01_ventanas_sin_ml.py` | **Job 1.** Cuenta eventos por ventana (por etiqueta y total, con ratio de ataque). Sin modelo |
| `jobs/02_inferencia_onnx.py` | **Job 2.** Clasifica cada evento con el modelo ONNX y publica en el topic `predictions` |

Hazlo en este orden: **primero el Job 1** (valida Kafka → Flink sin ML) y **después el Job 2**. Si algo falla en el Job 2 sabrás que el problema es el modelo y no la tubería.

## Puesta en marcha

Requisitos: Docker Desktop y, para el Job 2, haber ejecutado el notebook (genera `models/hotpath_rf_v1.onnx` y `.json`).

```bash
# 1. Levantar Kafka + Flink (la primera vez construye la imagen: varios minutos)
docker compose --profile flink up -d --build

# 2. Web UI de Flink: http://localhost:8081  (debe verse 1 TaskManager con 2 slots)

# 3. Lanzar el Job 1
docker compose exec jobmanager flink run -d -py /opt/job/jobs/01_ventanas_sin_ml.py --no-wait

# 4. En otra terminal (en tu maquina, con el venv): enviar eventos
python producer.py --csv data/Wednesday-workingHours.pcap_ISCX.csv --skip 300000 --limit 2000

# 5. Ver la salida del job
docker compose logs -f taskmanager
```

Verás líneas como `por_ventana> +I[2026-..., 2026-..., 24, 24, 1.0]`: ventana de inicio, fin, eventos, ataques y ratio de ataque.

### Job 2 (con modelo)

```bash
docker compose exec jobmanager flink run -d -py /opt/job/jobs/02_inferencia_onnx.py --no-wait
python consumer.py --topic predictions     # contador de ataques segun el modelo
```

Cada predicción trae `true_label`, `predicted_label`, `score`, `infer_ms` (tiempo del modelo) y `e2e_ms` (desde que Kafka recibió el evento hasta que Flink lo clasificó).

### Parar y reiniciar

| Quiero | Comando |
|---|---|
| Ver los jobs | `docker compose exec jobmanager flink list` |
| Parar un job | `docker compose exec jobmanager flink cancel <JOB_ID>` |
| Apagar todo | `docker compose --profile flink down` |
| Empezar de cero | `docker compose --profile flink down` (Kafka no guarda volumen: el topic se borra) |

## Modos de tiempo (`--time-mode`)

| Modo | Reloj de las ventanas | Cuándo usarlo |
|---|---|---|
| `ingest` (por defecto) | Hora a la que Kafka recibió el registro | Casi siempre: es monótona y no depende de los timestamps de 2017 |
| `event` | `timestamp_ms` del dataset | Si quieres reproducir el tiempo real de la captura |
| `proc` | Reloj de la máquina | Para demos: las ventanas cierran aunque dejen de llegar eventos |

**Por qué `ingest` por defecto:** si un solo timestamp del CSV estuviera mal (horas sin AM/PM), el *watermark* saltaría 12 horas y Flink descartaría como "tardíos" todos los eventos siguientes, sin avisar. Con `ingest` eso no puede pasar.

**Aviso de ventanas:** en `ingest` y `event` una ventana solo se cierra cuando llega un evento posterior. Al terminar el productor, la **última ventana no se emite**. Es el comportamiento esperado; con `--time-mode proc` sí se emite.

## Qué está verificado y qué no

| Pieza | Estado |
|---|---|
| DDL generado desde `schema_v1.py`, parseo JSON con `null` | Verificado (PyFlink 1.20.5 local, fuente de fichero) |
| Job 1: ventanas por etiqueta y total | Verificado (fuente de fichero, modo `event`) |
| Job 2: UDF ONNX, 3000 eventos | Verificado: **coincide al 100 %** con scikit-learn, incluidas filas con `null` |
| Modelo exportado en Python 3.13 / NumPy 2 y cargado en Python 3.11 / NumPy 1.24 | Verificado |
| `e2e_ms` con zona horaria | Verificado tras corregir un desfase de 2 h (ahora estable con distintas zonas) |
| **`docker compose build` de la imagen** | **No verificado** (sin Docker en mi entorno) |
| **Versión del JAR del conector de Kafka (`3.4.0-1.20`)** | **No verificado** (sin acceso a Maven) |
| **Lectura/escritura real contra Kafka** | **No verificado** |
| **`flink run -py` con argumentos (`--no-wait`)** | **No verificado** |

Si algo de lo no verificado falla, estas son las causas más probables:

| Síntoma | Causa y arreglo |
|---|---|
| El build falla en el `curl` del JAR (`404`) | La versión del conector no existe. Busca en Maven Central `flink-sql-connector-kafka` la que termine en `-1.20` y cámbiala en `KAFKA_CONNECTOR_VERSION` del Dockerfile |
| `Could not find any factory for identifier 'kafka'` | El JAR no está en `/opt/flink/lib`. Revisa el build; hay que reiniciar los contenedores tras reconstruir |
| El build falla en `assert sys.version_info < (3, 12)` | La imagen base trae un Python más nuevo que el que admite PyFlink 1.20. Cambia a una imagen base basada en Ubuntu 22.04 o instala `python3.11` |
| `Connection refused` a `localhost:9092` desde Flink | Dentro de Docker se usa `kafka:19092`, **no** `localhost:9092` (es el valor por defecto de `--bootstrap`) |
| `ModuleNotFoundError: schema_v1` | Los volúmenes de `docker-compose.yml` no se han montado; ejecuta los comandos desde la raíz del repo |
| `flink run` rechaza `--no-wait` | Quita el argumento del comando y pon `default=True` en `--no-wait` dentro de `flink_common.py` |
| El Job 2 falla al arrancar con "orden de features" | El modelo se entrenó con otro `schema_v1.py`. Reentrena con el notebook |
| `FileNotFoundError` del `.onnx` | No has ejecutado el notebook, o `models/` está vacía |
| Al relanzar el job se vuelven a leer eventos antiguos | Es `--startup earliest-offset`. Usa `--startup latest-offset` o `docker compose --profile flink down` |

## Siguiente paso

Sustituir los sinks `print` por una base de datos (Postgres o InfluxDB) y conectar Grafana. Ambos jobs ya generan las columnas que haría falta graficar: ratio de ataque por ventana, ataques predichos y latencia.
