"""
Job 2 - Clasificacion en streaming con el modelo ONNX.

Cada evento de `network-events` pasa por un UDF de Python que carga el modelo UNA vez
(en open()) y devuelve etiqueta predicha, confianza, tiempo de inferencia y latencia
desde que Kafka recibio el evento.

Salidas:
  - sink 'print'  (logs del taskmanager), siempre.
  - topic Kafka `predictions` (JSON), salvo --no-kafka-sink o --source file.
    Es el punto de enganche del siguiente paso (BD + Grafana) y tambien lo puede
    leer el Cold Path (arquitectura Kappa: Kafka sigue siendo la fuente de verdad).

Ejecucion (dentro del contenedor):
    flink run -d -py /opt/job/jobs/02_inferencia_onnx.py --no-wait
"""

import json
import time
from datetime import timezone

from pyflink.common import Row
from pyflink.table import DataTypes
from pyflink.table.udf import ScalarFunction, udf

from flink_common import common_args, create_source, make_env, run
from schema_v1 import FEATURE_FIELDS

DEFAULT_MODEL = "/opt/job/models/hotpath_rf_v1.onnx"
DEFAULT_META = "/opt/job/models/hotpath_rf_v1.json"


class HotPathClassifier(ScalarFunction):
    """eval(ts, *26 features) -> ROW<label, score, infer_ms, e2e_ms>"""

    def __init__(self, model_path, meta_path, time_mode):
        self.model_path = model_path
        self.meta_path = meta_path
        self.time_mode = time_mode

    def open(self, function_context):
        # Los imports van aqui: se ejecutan en el worker, una vez por tarea.
        import numpy as np
        import onnxruntime as ort

        self.np = np
        with open(self.meta_path, encoding="utf-8") as f:
            meta = json.load(f)
        # El modelo recibe un array SIN nombres: si el orden no coincide predice basura sin error.
        if meta["feature_order"] != FEATURE_FIELDS:
            raise RuntimeError("El orden de features del modelo no coincide con schema_v1.py. "
                               "Reentrena el modelo o corrige el esquema.")
        so = ort.SessionOptions()
        so.intra_op_num_threads = 1  # un evento suelto: mas hilos solo anaden overhead
        self.session = ort.InferenceSession(self.model_path, so, providers=["CPUExecutionProvider"])

    def eval(self, ts, *features):
        np = self.np
        x = np.array([[np.nan if v is None else v for v in features]], dtype=np.float32)
        t0 = time.perf_counter()
        out = self.session.run(None, {"input": x})
        infer_ms = (time.perf_counter() - t0) * 1000.0
        label = str(out[0][0])
        score = float(np.max(out[1][0]))
        e2e_ms = None
        if self.time_mode == "ingest" and ts is not None:
            # PyFlink entrega TIMESTAMP_LTZ como datetime sin zona; con table.local-time-zone=UTC es hora UTC
            ts_utc = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
            e2e_ms = time.time() * 1000.0 - ts_utc.timestamp() * 1000.0
        return Row(label, score, infer_ms, e2e_ms)


def main():
    p = common_args("Job 2: inferencia ONNX")
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--meta", default=DEFAULT_META)
    p.add_argument("--no-kafka-sink", action="store_true")
    p.add_argument("--out-topic", default="predictions")
    args, _ = p.parse_known_args()

    t_env = make_env()
    create_source(t_env, args)

    result_type = DataTypes.ROW([
        DataTypes.FIELD("label", DataTypes.STRING()),
        DataTypes.FIELD("score", DataTypes.DOUBLE()),
        DataTypes.FIELD("infer_ms", DataTypes.DOUBLE()),
        DataTypes.FIELD("e2e_ms", DataTypes.DOUBLE()),
    ])
    classify = udf(HotPathClassifier(args.model, args.meta, args.time_mode),
                   input_types=[DataTypes.TIMESTAMP_LTZ(3)] + [DataTypes.DOUBLE()] * len(FEATURE_FIELDS),
                   result_type=result_type)
    t_env.create_temporary_function("classify", classify)

    sink_cols = """
        timestamp_ms BIGINT, true_label STRING, predicted_label STRING, predicted_attack BOOLEAN,
        score DOUBLE, infer_ms DOUBLE, e2e_ms DOUBLE
    """
    t_env.execute_sql(f"CREATE TABLE predictions_print ({sink_cols}) "
                      "WITH ('connector' = 'print', 'print-identifier' = 'prediccion')")
    use_kafka = args.source == "kafka" and not args.no_kafka_sink
    if use_kafka:
        t_env.execute_sql(f"""
            CREATE TABLE predictions_kafka ({sink_cols}) WITH (
                'connector' = 'kafka', 'topic' = '{args.out_topic}',
                'properties.bootstrap.servers' = '{args.bootstrap}', 'format' = 'json'
            )""")

    feats = ", ".join(f"CAST({f} AS DOUBLE)" for f in FEATURE_FIELDS)
    select = f"""
        SELECT timestamp_ms, label AS true_label, p.label AS predicted_label,
               (p.label <> 'BENIGN') AS predicted_attack, p.score, p.infer_ms, p.e2e_ms
        FROM (SELECT timestamp_ms, label, classify(ts, {feats}) AS p FROM network_events)
    """
    stmt = t_env.create_statement_set()
    stmt.add_insert_sql("INSERT INTO predictions_print " + select)
    if use_kafka:
        stmt.add_insert_sql("INSERT INTO predictions_kafka " + select)
    run(stmt, args)


if __name__ == "__main__":
    main()
