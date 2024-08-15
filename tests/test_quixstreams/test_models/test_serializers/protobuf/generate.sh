# This helper script will convert *.proto into *_pb2.py
# and fix the annoying absolute import problem
# (more at https://github.com/protocolbuffers/protobuf/issues/1491)

# Usage:
# $ cd cd tests/test_quixstreams/test_models/protobuf
# $ ./generate.sh

# Generate Python code for all .proto files in the current directory
for proto_file in *.proto; do
  protoc --python_out=. "$proto_file"
done

# Fix the import paths in the generated *_pb2.py files
for pb2_file in *_pb2.py; do
  if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS version of sed (BSD sed)
    sed -i '' -E 's/^import ([a-zA-Z_][a-zA-Z0-9_]*)_pb2 as (.*)/from . import \1_pb2 as \2/' "$pb2_file"
  else
    # GNU sed (Linux, etc.)
    sed -i -E 's/^import ([a-zA-Z_][a-zA-Z0-9_]*)_pb2 as (.*)/from . import \1_pb2 as \2/' "$pb2_file"
  fi
done