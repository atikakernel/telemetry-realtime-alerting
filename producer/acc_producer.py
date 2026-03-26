import json
import socket
import logging
from kafka import KafkaProducer
import sys
import time

# Configuration
UDP_IP = "0.0.0.0"
UDP_PORT = 9003
KAFKA_BROKER = "localhost:9092"
TOPIC = "telemetry-acc"

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("🏎️ ACC Kafka Producer starting...")
    logger.info(f"   Listening on UDP {UDP_IP}:{UDP_PORT}")
    logger.info(f"   Forwarding to Kafka {KAFKA_BROKER} topic: {TOPIC}")

    # Initialize Kafka Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Retries and resilience
            retries=5,
            acks='all'
        )
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
        sys.exit(1)

    # Initialize UDP Socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))

    logger.info("🚀 Producer ready. Waiting for ACC data...")

    try:
        while True:
            data, addr = sock.recvfrom(65535)
            try:
                packet = json.loads(data.decode('utf-8'))
                
                # Filter essential fields for the alerting pipeline
                telemetry_event = {
                    "timestamp": packet.get("timestamp", time.time()),
                    "packetId": packet.get("packetId", 0),
                    "rpms": packet.get("rpms", 0),
                    "speedKmh": packet.get("speedKmh", 0),
                    "gear": packet.get("gear", 0),
                    "throttle": packet.get("throttle", 0.0),
                    "brake": packet.get("brake", 0.0),
                    "car": packet.get("car", "Unknown"),
                    "track": packet.get("track", "Unknown")
                }

                # Send to Kafka
                producer.send(TOPIC, telemetry_event)
                
                # Optional: log every 100 packets
                if telemetry_event["packetId"] % 100 == 0:
                    logger.info(f"📡 Packet {telemetry_event['packetId']} sent | RPM: {telemetry_event['rpms']} | Speed: {int(telemetry_event['speedKmh'])} km/h")

            except json.JSONDecodeError:
                logger.warning("⚠️ Received invalid JSON packet")
            except Exception as e:
                logger.error(f"❌ Error processing packet: {e}")

    except KeyboardInterrupt:
        logger.info("🛑 Shutting down producer...")
    finally:
        sock.close()
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
