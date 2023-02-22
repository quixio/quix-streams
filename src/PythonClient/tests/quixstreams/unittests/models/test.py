from quixstreams import SecurityOptions, KafkaStreamingClient

security = SecurityOptions(CERTIFICATES_FOLDER, QUIX_USER, QUIX_PASSWORD)
client = KafkaStreamingClient('kafka-k1.quix.ai:9093,kafka-k2.quix.ai:9093,kafka-k3.quix.ai:9093', properties=)