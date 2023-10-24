# `StreamingDataFrames` Examples

This folder contains a few examples/boiler-plate applications to get you started with
the `StreamingDataFrames` library.

## Running an Example

Simply pick a folder in here, like `bank_example`. Then, a serialization type, like 
`json`. Then, run any desired app in that folder  via `python examples/path/to/app.py`

## Requirements

- Working from the examples directory (`/quix-streams/src/StreamingDataFrames/examples`)
  - that includes executing python files from here
- Installing the python requirements file in here
  - `pip install -r requirements.txt`
- A Kafka instance to connect to, which has topic creation privileges.
  - You can use the easy-to-use one included here via `docker-compose up -d`
    - Has a UI at `localhost:9021`
    - Kill with `docker-compose down` when finished
