# `StreamingDataFrames` Example

This folder contains a few examples/boiler-plate applications to get you started with
the `StreamingDataFrames` library.

## Prerequisites

- Working from the examples directory (`/quix-streams/src/StreamingDataFrames/examples`)
- Installing the requirements `pip install -r requirements.txt` (installs this library)
- A Kafka instance to connect to, which has topic creation privileges.
  - You can use the easy-to-use one included here via `docker-compose up -d`
    - Has a UI at `localhost:9021`
    - Kill with `docker-compose down` when finished

## Intended Use

First, any executions related to these examples should be done from this 
(`./examples`) directory.

You will see a couple `.env` files around: in general, you should add/replace the
given values in them with your own if you are running any app in that folder. 

If you are running a local kafka cluster via `docker-compose up -d`, 
everything should work by default (not platform-specific examples, of course).

## Overview of Example

### Purpose

This will showcase:

  - How to use multiple kafka services together
    - Producer
    - `streamingdataframes.Application` (consume + produce)
  - Basic usage of the `dataframe` object
  - Using different serializations/data structures
    - json
    - Quix types (more intended for platform use)
  - Other general boilerplate

Also, if using the Quix Platform:
  - How to use the "Quix" version of an application (`Application.Quix`)
    - help highlight its differences
    - How to connect to the platform (particularly, locally)
  - Using the Quix serializers (backwards-compatible with `quixstreams<v0.6`)

### Structure

The current example is a simple suite of applications, comprised
of a producer (to generate messages), and a downstream `streamingdataframes.Application` 
that consumes said messages, processes them, and produces a new result downstream.

Each application/file is prefixed with the _serialization_, and there are also 
"general" and "platform"-specific examples. You should combine the same serialization 
and platform types together.

For example, use `./examples/producers/json_producer.py` alongside 
`./examples/applications/json_application.py`.

The apps in `./examples/platforms/quix` can be ran together in a similar fashion.

### Summary of Logic

In this example, imagine we are a very simple bank with various accounts. We also have
"Silver" and "Gold" memberships, where "Gold" members have extra perks as we'll see!

Our `producer` is mimicking attempted customer "transactions" aka purchases 
by generating and producing random customer transactions.

Our `streamingdataframes.Application`, downstream of the `producer`, is going to notify
said customer of the transaction attempt, but only if that customer has a "Gold" 
account and the purchase attempt was above a certain cost (we don't want to spam them!).


## Running an example

Ensure all prerequisites and intended use directions are followed.

Then, simply call `python /path/to/an/example.py`