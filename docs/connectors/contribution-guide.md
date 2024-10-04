# Contributing new Connectors

Quix Streams provide APIs to build custom Sources and Sinks.

In this guide, we will explain how to contribute a new Source or Sink to Quix Streams
and share your work with the community.

## Where to start
Before submitting a new connector, we encourage you to try out Quix Streams
and build an application with it. 

It will help you to better understand how the library works.

Check out our [Tutorials](../tutorials/README.md) to get started. 

## Steps

1. Check the [list of GitHub issues with a `connector` label](https://github.com/quixio/quix-streams/labels/connector) to see if a request for the connector has already been submitted.
2. If there is no issue, create a new one named `"New {source/sink}: {name}"`, and describe your idea. 
3. At this point, you can either ask for help from Quix Streams maintainers, or implement it yourself and contribute it to the library.  
If you want to develop it yourself, proceed to the next section.


### Contributing a new Sink or Source

1. Fork [the Quix Streams repository](https://github.com/quixio/quix-streams/).

2. Clone the repo locally.

3. Create a branch for your feature or bug in the format `{feature}/{connector-name}` (e.g., `feature/mqtt-source`)

4. Read ["Create a custom source"](sources/custom-sources.md) and ["Create a custom sink"](sinks/custom-sinks.md) to learn how to build a connector.

5. Create a new Python module in `quixstreams/sources/community/{connector-name}` for Sources or `quixstreams/sinks/community/{connector-name}` for Sinks.

6. In this module, add a new class for your connector following the format `"{Technology}Sink"` or `"{Technology}Source"` (e.g. `MQTTSource` , `PostgreSQLSink` ).  
Read the [Best Practices](#best-practices-for-the-connectors-code) section on how to structure your code and test it.

7. Test your changes locally in some sandbox environment to make sure it works. 

8. Commit your changes and push to your fork.

9. Create a Pull Request to Quix Streams `main` branch.
See [How to describe your PR](#how-to-describe-your-pr) section to learn how to structure the PR.
   
10. Wait for a feedback from a Quix Streams maintainer and adjust the code if necessary.

11. Feel free to join `#contributors` channel in [the community Slack](https://quix.io/slack-invite) and announce your work!

12. After the PR is accepted, the maintainers will take care releasing it in the next version of Quix Streams.


## Best practices for the connector's code

### Adding external libraries
In many cases, the connector needs an external library to work.

These libraries are considered optional, and they should not be required for the default
`quixstreams` installation.

When using an external library:

1. Add new optional dependency to `pyproject.toml` in the `[project.optional-dependencies]`
section.  
For example, for MQTTSource you need to add `mqtt = [paho-mqtt>=2.1.0,<3]`.
2. Add a dependency to `tests/requirements.txt` for the tests to run.
3. To provide nice exception messages for users, wrap the library import in try-except in the connector's code like this:

```python
try:
    import influxdb_client_3
except ImportError as exc:
    raise ImportError(
        'Package "influxdb3-python" is missing: '
        "run pip install quixstreams[influxdb3] to fix it"
    ) from exc
```


### Writing docstrings
Add a docstring to your connector class briefly describing how it works and covering
parameters it accepts.

Look at [InfluxDB3Sink](../api-reference/sinks.md#influxdb3sink) as an example. 

### Writing tests
Add unit tests to the `tests/test_quixstreams/test_{sources/sinks}/test_community/test_{connector-name}` folder.  
Use tests for existing connectors in `tests/test_quixstreams/test_{sources/sinks}/test_core/*` folders as examples.


## How to describe your PR
To streamline the review process, describe your PR in the following format

- Link the PR to the previously created issue.
- Describe new libraries used by the connector.
- Briefly describe how the connector works, and what users need to know before using it.
- Describe the processing guarantees the connector provides (e.g., at-least-once, at-most-once, exactly-once).
- Add examples how to instantiate the connector class.
- Explain how to set up a sandbox environment for Quix Streams maintainers to test your PR.
