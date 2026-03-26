FROM bitnami/spark:3.5.1

WORKDIR /opt/bitnami/spark

# Install Python requirements if any (optional for now)
# COPY requirements.txt .
# RUN pip install -r requirements.txt

COPY spark_processor.py .

# Add Kafka connector (downloaded at runtime by Spark if using --packages)
# For a more robust Dockerfile, we could pre-download the JARs

CMD ["/opt/bitnami/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "spark_processor.py"]
