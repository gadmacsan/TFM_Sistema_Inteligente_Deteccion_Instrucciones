# Esquema Kafka — `schema_v1`

**Proyecto:** Plataforma Integral de Ciberseguridad (TFM A — Hot Path / TFM B — Cold Path)
**Fuentes:** CICIDS2017 (día Wednesday-workingHours, DoS/Heartbleed) y CSE-CIC-IDS2018 (un día DoS **por decidir**, ver la lista de pendientes)
**Estado:** v1 — versión inicial, pendiente de validar en sesión conjunta
**Implementación en código:** `schema_v1.py` (lista de campos, tipos, mapeo de columnas y limpieza). Es la fuente única que usan el productor, el notebook y Flink; este documento es su explicación.

## Propósito de este documento

Este esquema define la estructura única de los eventos que circulan por el topic de Kafka compartido entre el TFM A (detección en tiempo real) y el TFM B (analítica histórica). Kafka actúa como fuente única de verdad de la arquitectura Kappa, así que **cualquier cambio a este esquema debe crear una versión nueva (`schema_v2`, etc.), nunca modificar esta directamente** — así ningún consumidor se rompe por un cambio hecho por el otro subequipo. Ver la sección "Cómo evolucionar este esquema" al final.

## Por qué solo 30 campos (27 columnas del dataset + 3 de metadatos, de los ~78 originales)

Se seleccionaron los campos que responden a una de estas cuatro preguntas, que son las que distinguen tráfico de ataque de tráfico normal en DoS/DDoS:

1. **¿Cuánto?** — volumen de datos y paquetes
2. **¿Qué tan rápido?** — tasa de transmisión
3. **¿Fue una máquina?** — uniformidad de tamaño de paquete y ritmo de envío
4. **¿Cómo terminó?** — flags TCP de la conexión

Cualquier columna del dataset original que no aportara una señal nueva a estas cuatro preguntas (percentiles finos de duración activa/idle, subflow stats, bulk rate, etc.) se dejó fuera para mantener el payload ligero — importante para la latencia del Hot Path.

## Campos

### Metadatos

| Campo | Tipo | Descripción |
|---|---|---|
| `schema_version` | string | Versión del esquema que usa este mensaje (`"v1"`) |
| `source_dataset` | string | `"cicids2017"` o `"cse-cic-ids2018"` — imprescindible para el análisis comparativo del TFM B |
| `timestamp_ms` | integer | Epoch en milisegundos, UTC. Ver nota de formato más abajo |

### Identificación de flujo

| Campo | Tipo | Descripción |
|---|---|---|
| `dst_port` | integer | Puerto de destino de la conexión |
| `protocol` | integer | Protocolo de transporte (6=TCP, 17=UDP, 0=otros) |

### Volumen

| Campo | Tipo | Descripción |
|---|---|---|
| `flow_duration` | integer | Duración del flujo en microsegundos |
| `tot_fwd_pkts` | integer | Total de paquetes enviados (cliente → servidor) |
| `tot_bwd_pkts` | integer | Total de paquetes de respuesta (servidor → cliente) |
| `totlen_fwd_pkts` | integer | Suma de bytes en dirección forward |
| `totlen_bwd_pkts` | integer | Suma de bytes en dirección backward |

### Tasa

| Campo | Tipo | Descripción |
|---|---|---|
| `flow_byts_s` | float | Bytes por segundo del flujo completo |
| `flow_pkts_s` | float | Paquetes por segundo del flujo completo |

### Tamaño de paquete

| Campo | Tipo | Descripción |
|---|---|---|
| `fwd_pkt_len_mean` | float | Tamaño medio de paquete, dirección forward |
| `fwd_pkt_len_std` | float | Desviación estándar del tamaño, dirección forward |
| `bwd_pkt_len_mean` | float | Tamaño medio de paquete, dirección backward |
| `bwd_pkt_len_std` | float | Desviación estándar del tamaño, dirección backward |
| `pkt_len_mean` | float | Tamaño medio de paquete, ambas direcciones |
| `pkt_len_std` | float | Desviación estándar del tamaño, ambas direcciones |

### Burstiness (patrón de ráfaga)

| Campo | Tipo | Descripción |
|---|---|---|
| `flow_iat_mean` | float | Media del tiempo entre paquetes consecutivos (Inter-Arrival Time) |
| `flow_iat_std` | float | Desviación estándar del tiempo entre paquetes |

### Flags TCP

| Campo | Tipo | Descripción |
|---|---|---|
| `syn_flag_cnt` | integer | Nº de paquetes con flag SYN |
| `ack_flag_cnt` | integer | Nº de paquetes con flag ACK |
| `rst_flag_cnt` | integer | Nº de paquetes con flag RST |
| `fin_flag_cnt` | integer | Nº de paquetes con flag FIN |

### Asimetría

| Campo | Tipo | Descripción |
|---|---|---|
| `down_up_ratio` | float | Ratio entre tráfico de bajada y de subida |
| `init_fwd_win_byts` | integer | Tamaño de ventana TCP inicial anunciada por el cliente |
| `init_bwd_win_byts` | integer | Tamaño de ventana TCP inicial anunciada por el servidor |

### Actividad de sesión

| Campo | Tipo | Descripción |
|---|---|---|
| `active_mean` | float | Tiempo medio de actividad continua antes de una pausa |
| `idle_mean` | float | Tiempo medio de inactividad entre ráfagas |

### Etiqueta

| Campo | Tipo | Descripción |
|---|---|---|
| `label` | string | Clase real del flujo (`"BENIGN"`, `"DoS Hulk"`, `"DDoS"`, etc.). **`BENIGN` siempre en mayúsculas**: 2018 escribe `Benign`, y `normalize_label()` de `schema_v1.py` lo unifica. Las etiquetas de ataque se conservan tal cual |

### Valores no numéricos

Los CSV originales traen `NaN` e `Infinity` (sobre todo en `flow_byts_s` y `flow_pkts_s`, cuando la duración es 0). **En Kafka viajan como `null`**, nunca como texto, porque un string en un campo numérico rompe el parseo en Flink y en Spark. Los consumidores deben tolerar `null` en cualquier campo numérico.

## Formato de `timestamp_ms`

- **Tipo:** entero, epoch en milisegundos, UTC.
- **Por qué:** Flink trabaja nativamente en epoch millis para event-time processing (watermarks, ventanas), evita ambigüedad de zona horaria entre Python/Java/Power BI, y es más rápido de ordenar/filtrar en Spark que un string de fecha.
- **Aviso al parsear los CSV originales:** el timestamp de texto en los CSV de CICIDS2017/CSE-CIC-IDS2018 no trae zona horaria explícita, y el formato **no es consistente entre todos los ficheros de día** (algunos en 24h, otros con AM/PM). Como la hora real de captura no es relevante para el TFM, la convención acordada es: tratar cada timestamp original como si ya fuera UTC al convertirlo a epoch millis, y ser consistentes en todo el pipeline. Esta suposición debe quedar documentada en la memoria de ambos TFM.
- **Se fuerza UTC en el código** (`parse_timestamp_to_ms`), no la hora local de la máquina. Una primera versión del productor usaba la hora local y daba un epoch distinto en cada portátil (y con cambio de hora de verano). Si vuestras compañeras generan datos con otro código, deben hacer lo mismo.
- **Horas de tarde sin AM/PM (supuesto a validar):** algunos ficheros de 2017 escriben la tarde en formato 12 h sin marcador (`1:05` en lugar de `13:05`). Como la captura fue en horario laboral, `schema_v1.py` suma 12 h a cualquier hora anterior a las `WORKDAY_START_HOUR = 8` cuando no hay AM/PM. **Está probado con datos sintéticos, no con vuestro CSV real**: antes de darlo por bueno hay que mirar el histograma de horas del notebook (sección 2). El productor lo desactiva con `--no-hour-fix`.
- **El CSV no está en orden cronológico estricto.** El productor ordena por `timestamp_ms` las filas que reproduce. Quien consuma el topic no debe asumir orden perfecto: usad tolerancia de desorden en los *watermarks*.
- **Latencia extremo a extremo:** no hace falta un campo `ingest_ts`. Kafka guarda por sí mismo la hora a la que recibió cada registro (*record timestamp*), y Flink la expone como columna de metadatos. Evita cambiar el esquema.

## Pendiente de resolver en la sesión conjunta de esquema

- [ ] Verificar que las cadenas de `label` coincidan literalmente entre CICIDS2017 y CSE-CIC-IDS2018 (p. ej. `"DoS Hulk"` vs `"DoS-Hulk"`) — si no coinciden, el conteo por categoría del TFM B se rompe en silencio. `BENIGN`/`Benign` ya está resuelto en código; falta comprobar las etiquetas de ataque.
- [ ] Elegir el día de 2018 para el test externo. Candidatos DoS: 15 y 16 de febrero. Hay problemas documentados en algunos días de 2018 (por ejemplo en el de 16 de febrero), así que **hay que revisar en el CSV real cuántos flujos de ataque trae cada día antes de decidir**.
- [ ] Definir el mapeo de columnas de 2018 (cabeceras abreviadas: `Dst Port`, `Tot Fwd Pkts`, `Flow Byts/s`...). Ese CSV no trae IPs y su fecha es `dd/mm/yyyy HH:MM:SS`.
- [ ] Validar el supuesto de las horas de tarde sin AM/PM con el CSV real (ver arriba).
- [ ] Confirmar si el TFM B necesita algún campo adicional para su forecasting no cubierto por este esquema.
- [ ] Acordar si las distintas versiones de esquema viajan en topics separados (`events-v1`, `events-v2`) o en el mismo topic distinguidas por `schema_version`.

## Cómo evolucionar este esquema

No se edita este documento ni el esquema en producción directamente. Para añadir o cambiar un campo:

1. Copiar este archivo a `schema_v2.md` con los cambios.
2. Actualizar `schema_version` a `"v2"` en los mensajes nuevos.
3. Los consumidores existentes (Flink, Fabric) siguen leyendo `schema_v1` hasta que cada equipo migre explícitamente — nunca se rompe nada el mismo día que cambia el esquema.

## Ejemplo de mensaje

```json
{
  "schema_version": "v1",
  "source_dataset": "cicids2017",
  "timestamp_ms": 1499421458000,
  "dst_port": 443,
  "protocol": 6,
  "flow_duration": 141385,
  "tot_fwd_pkts": 9,
  "tot_bwd_pkts": 7,
  "totlen_fwd_pkts": 553,
  "totlen_bwd_pkts": 3773,
  "flow_byts_s": 30597.31,
  "flow_pkts_s": 113.17,
  "fwd_pkt_len_mean": 61.44,
  "fwd_pkt_len_std": 87.53,
  "bwd_pkt_len_mean": 539.0,
  "bwd_pkt_len_std": 655.43,
  "pkt_len_mean": 254.47,
  "pkt_len_std": 474.71,
  "flow_iat_mean": 17673.13,
  "flow_iat_std": 23965.32,
  "syn_flag_cnt": 0,
  "ack_flag_cnt": 0,
  "rst_flag_cnt": 1,
  "fin_flag_cnt": 0,
  "down_up_ratio": 0,
  "init_fwd_win_byts": 8192,
  "init_bwd_win_byts": 119,
  "active_mean": 0,
  "idle_mean": 0,
  "label": "BENIGN"
}
```
