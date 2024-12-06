# Quix Streams Tutorials

Here are a few basic application patterns with accompanying walkthroughs 
to get you started with the Quix Streams library.

## Running a Tutorial

Should you wish to run one of the tutorials, follow its instructions at the bottom.

Besides a Kafka instance to connect to, most of them likely fit the pattern of:

`pip install quixstreams`

`python ./path/to/producer.py`

`python ./path/to/tutorial_app.py`


## Running Kafka Locally

First, make sure you have docker-compose installed. Then:

1. [Download this docker-compose file](./docker-compose.yml) to a preferred location
2. From its directory, do `docker-compose up -d`

It sets up everything automatically, running it in the background!
 
- You can connect your Kafka clients via `localhost:9092`
- It has a helpful UI at `http://localhost:9021`
    - allows you to easily inspect and manage topics

When finished, do `docker-compose down` from the download location to kill it.
