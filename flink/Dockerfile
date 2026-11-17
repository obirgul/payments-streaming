FROM flink:1.20.0-scala_2.12-java17
# Flink SQL icin Kafka connector (Flink 1.20 ile uyumlu surum)
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/
RUN chown flink:flink /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar
