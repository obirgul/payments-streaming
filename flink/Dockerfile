FROM flink:1.20.0-scala_2.12-java17
# Kafka connector for Flink SQL (version compatible with Flink 1.20)
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/
RUN chown flink:flink /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar
# Prometheus metrics reporter (loaded as a plugin; not bundled in the image)
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.20.0/flink-metrics-prometheus-1.20.0.jar /opt/flink/plugins/metrics-prometheus/
RUN chown -R flink:flink /opt/flink/plugins/metrics-prometheus
