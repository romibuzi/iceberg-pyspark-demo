FROM apache/spark-py:v3.4.0

USER root

ARG MAVEN_REPO=https://repo1.maven.org/maven2
ARG SPARK_VERSION=3.4
ARG SCALA_VERSION=2.13
ARG ICEBERG_VERSION=1.8.1

ARG AWS_VERSION=2.31.1
ARG POSTGRES_VERSION=42.7.5

RUN apt update && apt install -y wget

RUN wget $MAVEN_REPO/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_$SCALA_VERSION/$ICEBERG_VERSION/iceberg-spark-runtime-${SPARK_VERSION}_$SCALA_VERSION-$ICEBERG_VERSION.jar \
  -O $SPARK_HOME/jars/iceberg-spark-runtime-${SPARK_VERSION}_$SCALA_VERSION-$ICEBERG_VERSION.jar

RUN wget $MAVEN_REPO/software/amazon/awssdk/bundle/$AWS_VERSION/bundle-$AWS_VERSION.jar \
  -O $SPARK_HOME/jars/aws-bundle-$AWS_VERSION.jar

RUN wget $MAVEN_REPO/software/amazon/awssdk/url-connection-client/$AWS_VERSION/url-connection-client-$AWS_VERSION.jar \
  -O $SPARK_HOME/jars/aws-url-connection-client-$AWS_VERSION.jar

RUN wget $MAVEN_REPO/org/postgresql/postgresql/$POSTGRES_VERSION/postgresql-$POSTGRES_VERSION.jar \
  -O $SPARK_HOME/jars/postgresql-$POSTGRES_VERSION.jar

ENV AWS_ACCESS_KEY_ID=minioadmin
ENV AWS_SECRET_ACCESS_KEY=minioadmin
ENV AWS_REGION=eu-west-1

WORKDIR /opt/spark/work-dir/

CMD ["/opt/spark/bin/spark-submit", "main.py"]
