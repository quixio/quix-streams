# Quix Platform Users: `Application.Quix()`

For those using the Quix platform directly (that is, using the client in the web 
browser), all you need for ensuring everything connects as expected is to use the 
`Application.Quix()` instance.

There aren't many features unique to the `Quix` version other than allowing topics to
auto create via `auto_create_topics=True`, which is the default (Quix requires a special
API to create topics).

<br>

## Using Quix-formatted Messages (SERDES)

The Quix Platform/topics use their own messaging formats, known as `TimeseriesData` and 
`EventData`. In order to consume or produce these, you must use the respective SERDES:

- `QuixDeserializer` (can deserialize both)
- `QuixEventsSerializer`
- `QuixTimeseriesSerializer`

By default, they currently use the "legacy" format (Quixstreams<0.6.0), so we recommend 
using the flag `as_legacy=False` when you are ready to convert over to the newest format.

See [**Migrating from Legacy Quixstreams**](./upgrading_legacy.md) 
for more detail around the legacy format.

Of course, you can use whatever serialization method you like instead of the Quix 
format, it just may not play as nicely with the Quix Platform UI tooling!


<br>

## Using `Application.Quix()` externally

If you decide to connect to the Quix Platform from external sources (i.e. running an 
app connected to the platform directly via your local machine), you will need to set 
the following environment variables:

```
Quix__Sdk__Token
Quix__Portal__Api
```

You can find these values on the platform in your given workspace settings.
