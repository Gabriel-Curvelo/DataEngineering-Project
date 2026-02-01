FROM quay.io/astronomer/astro-runtime:9.1.0

COPY include/ /usr/local/airflow/include/

USER root

# Instala dependências e o OpenJDK 11 da Eclipse Temurin
RUN apt-get update && \
    apt-get install -y wget tar && \
    wget https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.20.1+1/OpenJDK11U-jdk_x64_linux_hotspot_11.0.20.1_1.tar.gz && \
    mkdir -p /usr/lib/jvm && \
    tar -xzf OpenJDK11U-jdk_x64_linux_hotspot_11.0.20.1_1.tar.gz -C /usr/lib/jvm && \
    rm OpenJDK11U-jdk_x64_linux_hotspot_11.0.20.1_1.tar.gz

# Define o JAVA_HOME e adiciona ao PATH
ENV JAVA_HOME=/usr/lib/jvm/jdk-11.0.20.1+1
ENV PATH="$JAVA_HOME/bin:$PATH"

USER astro
