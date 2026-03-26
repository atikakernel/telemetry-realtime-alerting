# Real-Time Telemetry Alerting 🏎️💨

Este proyecto implementa una arquitectura de telemetría en tiempo real para sim racing, utilizando Docker, Kafka y Spark Structured Streaming.

## Arquitectura

El flujo de datos es el siguiente:

```mermaid
graph LR
    A[Assetto Corsa Competizione] -->|UDP| B(Python Producer)
    B -->|Kafka| C[Kafka Broker KRaft]
    C --> D(Spark Structured Streaming)
    D -->|5s Window| E{Threshold Engine}
    E -->|RPM > 7500| F[Alert! 🚨]
```

### Componentes

1.  **Productor (Python)**: Escucha paquetes UDP de ACC y los envía a Kafka en tiempo real.
2.  **Broker (Kafka)**: Gestiona la ráfaga de datos entrantes (usando KRaft para simplicidad).
3.  **Consumidor (Spark Structured Streaming)**: Procesa los datos en ventanas de 5 segundos y dispara alertas críticas.

## Setup

1.  **Levantar Infraestructura**:
    ```bash
    docker-compose up -d
    ```

2.  **Ejecutar Productor**:
    ```bash
    cd producer && python producer.py
    ```

3.  **Ejecutar Consumidor**:
    ```bash
    cd spark-consumer && spark-submit spark_processor.py
    ```
