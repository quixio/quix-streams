## Documentation

API references for both the C# and Python clients are generated from the XML and docstrings in the code. C# uses [DefaultDocumentation](https://github.com/Doraku/DefaultDocumentation#Usage_DotnetTool), while Python uses [Pydoc Markdown](https://niklasrosenstein.github.io/pydoc-markdown/just-generate-me-some-markdown/). Follow the following steps to generate updated API references for Quix Streams.

### C# API Reference

After building the Quix Streams library, please follow the following steps to generate the C# API reference.

 - Install dotnet tool for DefaultDocumentation using the following command:
    ```
    dotnet tool install DefaultDocumentation.Console -g
    ```
- Clean up the directory containing documentation for the older version (if any):
    ```
    rm -r docs/api-reference/csharp/*
    ```
- Build QuixStreams.Streaming project
    ```
    dotnet build src/CsharpClient/QuixStreams.Streaming -c Release -f netstandard2.0
    ```
-  Run the following command from the root directory (or adjust the `-a` and `-o` flags accordingly):
    ```
    defaultdocumentation -s Public -a src/CsharpClient/QuixStreams.Streaming/bin/Release/netstandard2.0/QuixStreams.Streaming.dll -o docs/api-reference/csharp/ --FileNameFactory Name
    ```
    The above command generates API reference for `QuixStreams.Streaming` library in _docs/api-reference/csharp_ directory

### Python API Reference
 - Run the following command to install the CLI for Pydoc:
    ```
    pip install pydoc-markdown
    ```
- Run the following command from the root directory of the project to generate the API reference (or adjust the `-I` and file stdout is redirected to):
    ```
    pydoc-markdown -I src/PythonClient/src -p quixstreams --render-toc > docs/api-reference/python/quixstreams.md
    ```
