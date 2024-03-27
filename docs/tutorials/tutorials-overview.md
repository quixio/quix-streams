# Quix Streams Tutorials

This folder contains a few basic application patterns with accompanying walkthroughs 
to get you started with the Quix Streams library.

## Running a Tutorial

Should you wish to run one of the tutorials, follow the instructions as
listed at the bottom of it's `./tutorial.md`.

Besides a Kafka instance to connect to, most of them likely fit the pattern of:

`pip install quixstreams`

`python ./path/to/producer.py`

`python ./path/to/application.py`


## Running Kafka Locally

From this file's directory, just call `docker-compose up -d`

It sets up everything automatically!
 
- You can connect your Kafka clients via `localhost:9092`
- It has a helpful UI at `http://localhost:9021`
  - allows you to easily inspect and manage topics

When finished, do `docker-compose down` from this file's directory to kill it.
