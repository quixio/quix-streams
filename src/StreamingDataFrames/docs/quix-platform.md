# Quix Platform Users: `Application.Quix()`

If you are deploying your Quix Streams application to the Quix platform directly, all you need for ensuring everything connects as expected is to use the 
`Application.Quix()` instance.

`Application.Quix()` automatically configures the application to use Quix Kafka broker using API token, and it also provides API to than allowing topics to
auto create via `auto_create_topics=True` parameter.
<br>
By default, `Application.Quix()` use `auto_create_topics=True`.

<br>

## Using Quix-formatted Messages (SerDes)

The Quix Platform/topics use their own messaging formats, known as `TimeseriesData` and 
`EventData`. In order to consume or produce these, you must use the respective SerDes:

- `QuixDeserializer` (can deserialize both)
- `QuixEventsSerializer`
- `QuixTimeseriesSerializer`

See [**Migrating from Legacy Quix Streams**](./upgrading_legacy.md) 
for more detail around the Quix format.

You may use any serialization method you like instead of the Quix 
format, but Quix Platform provides additional features on top of this format in the UI.


<br>

## Using `Application.Quix()` locally

You may connect to Quix Platform locally too (e.g. while developing the application on your machine).
<br>
To do that, you will need to set the following environment variables:

```
Quix__Sdk__Token
Quix__Portal__Api
```

You can find these values on the platform in your Workspace settings.
