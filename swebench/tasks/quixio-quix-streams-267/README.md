# quixio-quix-streams-267

## Build
```bash
docker build --platform linux/amd64 -t quixio-quix-streams-267-test .
```

## Run pre-patch tests (should fail)
```bash
docker run --platform linux/amd64 --rm \
  -v $(pwd)/run_test.sh:/workspace/run_test.sh:ro \
  -v $(pwd)/test.patch:/tmp/test.patch:ro \
  quixio-quix-streams-267-test bash -c \
    'cd /workspace/repo && git apply /tmp/test.patch && \
    bash /workspace/run_test.sh'
```

## Run post-patch tests (should pass)
```bash
docker run --platform linux/amd64 --rm \
  -v $(pwd)/run_test.sh:/workspace/run_test.sh:ro \
  -v $(pwd)/test.patch:/tmp/test.patch:ro \
  -v $(pwd)/golden.patch:/tmp/golden.patch:ro \
  quixio-quix-streams-267-test bash -c \
    'cd /workspace/repo && git apply /tmp/test.patch && \
    git apply /tmp/golden.patch && bash /workspace/run_test.sh'
```
