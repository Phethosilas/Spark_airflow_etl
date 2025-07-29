FROM astrocrpublic.azurecr.io/runtime:3.0-6

# Set JAVA_HOME for Java 17
USER root
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Download and install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Add Spark to PATH
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

# Switch back to astro user
USER astro
