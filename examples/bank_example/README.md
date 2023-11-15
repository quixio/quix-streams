# Bank Example

## Summary of Logic

In this example, imagine we are a very simple bank with various accounts. We also have
"Silver" and "Gold" memberships, where "Gold" members have extra perks as we'll see!

Our `producer` is mimicking attempted customer "transactions" (aka purchases) 
by generating and producing random customer transactions.

Our `consumer`, downstream of the `producer`, is going to notify
said customer of the transaction attempt, but only if that customer has a "Gold" 
account and the purchase attempt was above a certain cost (we don't want to spam them!).


## Purpose

This example showcases:
  - How to use multiple Quix kafka applications together
    - Producer
    - Consumer via `quixstreams.Application` (consumes and produces)
  - Basic usage of the `StreamingDataFrame` object
  - Using different serializations/data structures
    - json
    - Quix serializers (more intended for Quix platform use)
      - backwards-compatible with previous Python client in `quixstreams<v0.6`
  - Other general boilerplate
