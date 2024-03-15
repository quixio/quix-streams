import os

from quixstreams.connectors.sources.csv.connector import CsvSourceConnector

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

csv_source_connector = CsvSourceConnector(
    csv_path=os.path.join(script_dir, "demo-data.csv"),
    producer_topic_name="csv_transaction_data",
    producer_broker_address="localhost:9092",
    key_extractor="AccountId",
)

if __name__ == "__main__":
    with csv_source_connector as connector:
        connector.run()
