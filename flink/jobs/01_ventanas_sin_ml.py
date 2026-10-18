"""
Job 1 - Ventanas sin ML.

Lee `network-events` de Kafka y cuenta eventos por ventana (tumbling). Sirve para
validar el camino Kafka -> Flink antes de meter el modelo, y las dos salidas son las
que alimentaran Grafana mas adelante.

  por_etiqueta : eventos por ventana y etiqueta real
  por_ventana  : eventos, ataques y ratio de ataque por ventana

Salida: sink 'print' (se ve en los logs del taskmanager).

Ejecucion (dentro del contenedor):
    flink run -d -py /opt/job/jobs/01_ventanas_sin_ml.py --no-wait
"""

from flink_common import common_args, create_source, make_env, run


def main():
    args, _ = common_args("Job 1: ventanas sin ML").parse_known_args()
    t_env = make_env()
    create_source(t_env, args)

    t_env.execute_sql("""
        CREATE TABLE window_by_label (
            window_start TIMESTAMP(3), window_end TIMESTAMP(3), label STRING, events BIGINT
        ) WITH ('connector' = 'print', 'print-identifier' = 'por_etiqueta')
    """)
    t_env.execute_sql("""
        CREATE TABLE window_total (
            window_start TIMESTAMP(3), window_end TIMESTAMP(3),
            events BIGINT, attacks BIGINT, attack_ratio DOUBLE
        ) WITH ('connector' = 'print', 'print-identifier' = 'por_ventana')
    """)

    tvf = f"TABLE(TUMBLE(TABLE network_events, DESCRIPTOR(ts), INTERVAL '{args.window_s}' SECOND))"
    stmt = t_env.create_statement_set()
    stmt.add_insert_sql(f"""
        INSERT INTO window_by_label
        SELECT window_start, window_end, COALESCE(label, '?') AS label, COUNT(*) AS events
        FROM {tvf}
        GROUP BY window_start, window_end, COALESCE(label, '?')
    """)
    stmt.add_insert_sql(f"""
        INSERT INTO window_total
        SELECT window_start, window_end,
               COUNT(*) AS events,
               CAST(SUM(CASE WHEN COALESCE(label, '?') <> 'BENIGN' THEN 1 ELSE 0 END) AS BIGINT) AS attacks,
               CAST(SUM(CASE WHEN COALESCE(label, '?') <> 'BENIGN' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS attack_ratio
        FROM {tvf}
        GROUP BY window_start, window_end
    """)
    run(stmt, args)


if __name__ == "__main__":
    main()
