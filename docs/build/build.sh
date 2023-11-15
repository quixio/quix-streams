set -ev

mkdir -p ../api-reference
pydoc-markdown > ../api-reference/quixstreams.md
