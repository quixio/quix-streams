# This python file is intended to be used as part of a build process initiated by build.py

import os
import subprocess
import sys

root="/QuixStreams"
os.system(f"cd {root}/src/builds/docs")

# C# API Reference
print("Generating C# docs")
os.system(f"rm -rf {root}/docs/api-reference/csharp")  # Clean up the directory containing documentation for the older version
os.system(f"mkdir {root}/docs/api-reference/csharp")
os.system(f"dotnet build {root}/src/CsharpClient/QuixStreams.Streaming -c Release -f netstandard2.0")  # Build QuixStreams.Streaming project
os.system(f"defaultdocumentation -s Public -a {root}/src/CsharpClient/QuixStreams.Streaming/bin/Release/netstandard2.0/QuixStreams.Streaming.dll -o {root}/docs/api-reference/csharp/ --FileNameFactory Name")  # Generate C# API reference

# Python API Reference
print("Generating python docs")
os.system(f"rm -rf {root}/docs/api-reference/python")  # Clean up the directory containing documentation for the older version
os.system(f"mkdir {root}/docs/api-reference/python")
os.system(f"pydoc-markdown -I {root}/src/PythonClient/src -p quixstreams --render-toc > {root}/docs/api-reference/python/quixstreams.md")  # Generate Python API reference
print("Docs generation done")