FROM confluentinc/cp-kafka-connect:7.4.0

RUN confluent-hub install --no-prompt --worker-configs /dev/null jcustenborder/kafka-connect-spooldir:2.0.65

RUN curl -fsSL -o /tmp/jmx_prometheus_javaagent.jar \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar

COPY jmx-config.yml /etc/kafka-connect/jmx-config.yml
