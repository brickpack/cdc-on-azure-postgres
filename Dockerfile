# Kafka Connect image for the Postgres -> Kafka CDC rollback pipeline.
#
# Base image version is pinned to match confluentinc/cp-kafka:7.5.0 used in
# docker/docker-compose.yml (same Confluent Platform release).
FROM confluentinc/cp-kafka-connect:7.5.0

ARG MYSQL_CONNECTOR_VERSION=8.0.33

# Debezium Postgres source connector (CDC capture) and the Confluent JDBC
# sink connector (idle rollback target) are installed via confluent-hub so
# their plugin manifests and transitive deps are managed correctly.
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.4.2 \
    && confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4 \
    && rm -rf /tmp/confluent-hub-*

# The Confluent Hub build of kafka-connect-jdbc ships WITHOUT a MySQL JDBC
# driver (GPL licensing prevents Confluent from bundling it). The JDBC sink
# is useless against MySQL without it, so it is added manually here rather
# than relying on confluent-hub.
#
# Offline build note: if the VM has no outbound internet access, download
# mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar yourself and replace this
# RUN line with:
#   COPY mysql-connector-j-8.0.33.jar /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/
RUN curl -fSL -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar \
    "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_CONNECTOR_VERSION}/mysql-connector-j-${MYSQL_CONNECTOR_VERSION}.jar"

ENV CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components
