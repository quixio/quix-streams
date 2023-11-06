# Upgrading from Legacy Quixstreams <v2.0.0

If you were using a previous version of QuixStreams, more specifically the Quix
`ParameterData` or `EventData` formats and plan on using old 
topics or mixing legacy + new library versions, then this section is for you!

***Note: Many of these details can be skipped for most users, but the context
may help you understand speed/operational differences you might experience, or if
you are an edge case that might require more work to convert.***

<br>

## Background

`quixstreams>=0.6` introduced some (currently optional) message formatting changes 
that are intended to be the default moving forward, but the old format is currently
still supported.

The intention of these changes is two-fold:
- Putting message-related metadata where it belongs - in the headers!
- Simplifying usage of the Quix protocol overall, particularly with 
`streamingdataframes`.

Let's highlight those differences, and other important things to be aware of while 
converting over to `streamingdataframes`.

<br>

## "Split" Messages Unsupported

`streamingdataframes` has deprecated the "split" messaging format (it will remain 
supported in C#).

Split messaging is only necessary in very specific circumstances with extremely large
messages, and requires a significant layer of complexity to provide desired processing 
guarantees going forward.

***`streamingdataframes` will (gracefully) skip any split messages it encounters.***

<br>

## Batched/"Buffered" Message Handling

`streamingdataframes` has deprecated producer-based batching/buffering of messages, 
which essentially consolidates multiple messages into one.

However, it WILL still be able to consume these messages without issue.

To process them, it will separate each record/message out and handle them individually. 

For example, a message with: 
```
{
  "NumericValues": {"p0": [10, 20, 30]},
  "StringValues": {p1": ["a", "b", "c"]}
}
```
would equate to 3 independently processed messages.


<br>

## Multiple Message/Serialization Types

The previous QuixStreams client supported and actively managed multiple message types 
on one topic under the hood, which significantly complicates the client messaging model.

It was mostly a way to handle things that we plan to handle differently in the future
with things like message headers and state.

As such, `streamingdataframes` intends to _encourage_ producing only one message type
per topic by enforcing 1 serializer per-topic, per-application. Additionally, 
`streamingdataframes` ignores all but `TimeseriesData` and `EventData` message types 
when using a `QuixDeserializer`. 

It will still be possible to handle multiple types in one application if you so wish, 
but it will require a lot of manual work (likely via `.apply()` functions) to handle 
both effectively.

It is also possible to handle them by just using different applications, where each
handles a specific message type.

In all, this is to discourage having many different message structures on one topic,
which is generally not a good design principal.

<br>

## `TimeseriesData` replaces `ParameterData`

Pre-0.6.0, we had what was known as `ParameterData`, with message value as:

```json
{
  "C": "JT",
  "K": "ParameterData",
  "V": {
    "Epoch": 1000000,
    "Timestamps": [10, 11, 12],
    "NumericValues": {
      "p0": [100.0, 110.0, 120.0],
      },
    "StringValues": {
      "p1": ["100", "110", "120"]
      },
    "BinaryValues": {
      "p2": ["MTAw", "MTEw", "MTIw"]
      }
  },
  "S": 34,
  "E": 251
}
```

<br>

This is now being replaced with a new format known as `TimeseriesData`:

```json
{
  "Epoch": 0,
  "Timestamps": [10, 11, 12],
  "NumericValues": {
    "p0": [100.0, 110.0, 120.0]
  },
  "StringValues": {
    "p1": ["100", "110", "120"]
  },
  "BinaryValues": {
    "p2": ["MTAw", "MTEw", "MTIw"]
  }
}
```

<br>

## EventData

```json
[
  {
    "Timestamp": 100,
    "Tags": {
      "tag1": "tagValue"
    },
    "Id": "event1",
    "Value": "value a"
  },
  {
    "Timestamp": 101,
    "Tags": {
      "tag1": "tagValue"
    },
    "Id": "event3",
    "Value": "value c"
  }
]
```
