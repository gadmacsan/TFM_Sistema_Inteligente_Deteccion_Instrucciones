# Imagen de Flink con PyFlink, el conector de Kafka y onnxruntime (Hot Path del TFM A).
#
# Construir:  docker compose --profile flink build
#
# Si cambias de version, cambia las dos juntas: FLINK_VERSION (imagen base y paquete
# apache-flink) y KAFKA_CONNECTOR_VERSION (formato <version conector>-<version Flink>).
ARG FLINK_VERSION=1.20.5
FROM flink:${FLINK_VERSION}
ARG FLINK_VERSION
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20

USER root

RUN apt-get update -y \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/python3 /usr/bin/python

# PyFlink 1.20 solo soporta Python <= 3.11. Si la imagen base trae uno mas nuevo, falla aqui con un mensaje claro.
RUN python3 -c "import sys; assert sys.version_info < (3, 12), 'PyFlink 1.20 requiere Python <= 3.11, esta imagen trae ' + sys.version"

# setuptools<81 + --no-build-isolation: algunas dependencias de apache-flink se compilan desde
# codigo fuente y necesitan pkg_resources, que los setuptools recientes ya no incluyen.
# numpy<2: lo exige PyFlink 1.20 (por eso el modelo se exporta a ONNX y no se usa joblib).
RUN pip3 install --no-cache-dir "setuptools<81" wheel \
 && pip3 install --no-cache-dir --no-build-isolation "apache-flink==${FLINK_VERSION}" "numpy<2" onnxruntime

# Conector SQL de Kafka. -f hace que el build FALLE si la URL no existe (version incorrecta).
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
 && chown flink:flink /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

USER flink
