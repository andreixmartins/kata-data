FROM confluentinc/cp-kafka-connect:7.7.1

RUN confluent-hub install --no-prompt --worker-configs /dev/null jcustenborder/kafka-connect-spooldir:2.0.65
RUN confluent-hub install --no-prompt --worker-configs /dev/null confluentinc/kafka-connect-jdbc:10.7.6

RUN mkdir -p /usr/share/java/kafka-connect-jdbc && \
    curl -fsSL -o /usr/share/java/kafka-connect-jdbc/postgresql-42.7.4.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

USER root

ENV CAMEL_VERSION=4.8.5
ENV CXF_VERSION=4.0.6
ENV CAMEL_PLUGIN_DIR=/opt/camel-connector/camel-cxf-kafka-connector

RUN mkdir -p /opt/camel-connector && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/camel/kafkaconnector/camel-cxf-kafka-connector/${CAMEL_VERSION}/camel-cxf-kafka-connector-${CAMEL_VERSION}-package.tar.gz" \
      -o /tmp/camel-cxf-connector.tar.gz && \
    tar -xzf /tmp/camel-cxf-connector.tar.gz -C /opt/camel-connector && \
    rm -f /tmp/camel-cxf-connector.tar.gz

RUN curl -fsSL -o ${CAMEL_PLUGIN_DIR}/cxf-rt-transports-http-undertow-${CXF_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/cxf/cxf-rt-transports-http-undertow/${CXF_VERSION}/cxf-rt-transports-http-undertow-${CXF_VERSION}.jar" && \
    curl -fsSL -o ${CAMEL_PLUGIN_DIR}/undertow-core-2.3.17.Final.jar \
      "https://repo1.maven.org/maven2/io/undertow/undertow-core/2.3.17.Final/undertow-core-2.3.17.Final.jar" && \
    curl -fsSL -o ${CAMEL_PLUGIN_DIR}/undertow-servlet-2.3.17.Final.jar \
      "https://repo1.maven.org/maven2/io/undertow/undertow-servlet/2.3.17.Final/undertow-servlet-2.3.17.Final.jar" && \
    curl -fsSL -o ${CAMEL_PLUGIN_DIR}/xnio-api-3.8.16.Final.jar \
      "https://repo1.maven.org/maven2/org/jboss/xnio/xnio-api/3.8.16.Final/xnio-api-3.8.16.Final.jar" && \
    curl -fsSL -o ${CAMEL_PLUGIN_DIR}/xnio-nio-3.8.16.Final.jar \
      "https://repo1.maven.org/maven2/org/jboss/xnio/xnio-nio/3.8.16.Final/xnio-nio-3.8.16.Final.jar" && \
    curl -fsSL -o ${CAMEL_PLUGIN_DIR}/jboss-threads-3.6.1.Final.jar \
      "https://repo1.maven.org/maven2/org/jboss/threads/jboss-threads/3.6.1.Final/jboss-threads-3.6.1.Final.jar" && \
    curl -fsSL -o ${CAMEL_PLUGIN_DIR}/wildfly-common-1.7.0.Final.jar \
      "https://repo1.maven.org/maven2/org/wildfly/common/wildfly-common/1.7.0.Final/wildfly-common-1.7.0.Final.jar" && \
    curl -fsSL -o ${CAMEL_PLUGIN_DIR}/jboss-logging-3.5.3.Final.jar \
      "https://repo1.maven.org/maven2/org/jboss/logging/jboss-logging/3.5.3.Final/jboss-logging-3.5.3.Final.jar"

RUN mkdir -p /tmp/shim/META-INF/services && \
    echo "org.apache.camel.kafkaconnector.cxf.CamelCxfSourceConnector" > /tmp/shim/META-INF/services/org.apache.kafka.connect.source.SourceConnector && \
    echo "org.apache.camel.kafkaconnector.cxf.CamelCxfSinkConnector" > /tmp/shim/META-INF/services/org.apache.kafka.connect.sink.SinkConnector && \
    cd /tmp/shim && jar cf ${CAMEL_PLUGIN_DIR}/camel-cxf-service-shim.jar META-INF/ && \
    rm -rf /tmp/shim

USER 1000

RUN curl -fsSL -o /tmp/jmx_prometheus_javaagent.jar \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar

COPY jmx-config.yml /etc/kafka-connect/jmx-config.yml
