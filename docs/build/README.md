# Build the Quix Streams docs

Generate API docs for `quixstreams` module using [Pydoc Markdown](https://niklasrosenstein.github.io/pydoc-markdown/just-generate-me-some-markdown/). 


## Generate new API docs

- Go to `docs/build`
- Install requirements via `python -m pip install -r requirements.txt`
- do `./build.sh`
- Check the generated docs in `docs/` folder


## Render/View Docs:
- Go to `docs/build`
- `python -m pip install mkdocs mkdocs-material mkdocs-material-extensions`
- `mkdocs serve -f ../../mkdocs.yml`
- [navigate to `localhost:8000` in browser](`http://localhost:8000`)
