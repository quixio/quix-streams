from pydoc_markdown.interfaces import Context
from pydoc_markdown import PydocMarkdown
from pydoc_markdown.contrib.source_linkers.git import GithubSourceLinker
from pydoc_markdown.contrib.processors.filter import FilterProcessor
from pydoc_markdown.contrib.loaders.python import PythonLoader
from pydoc_markdown.contrib.renderers.markdown import MarkdownRenderer


context = Context(
    directory=".",
)
loader = PythonLoader(search_path=["../.."], packages=["quixstreams"])
fp = "../api-reference/"


# direct
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# context = Context(
#     directory='.',
# )
# loader = PythonLoader(
#     # search_path=['.'],
#     packages=["quixstreams"],
#     # modules=["quixstreams.core.stream.functions"]
# )
# fp = "./docs/api-reference"

filter_ = FilterProcessor(
    expression="not name.startswith('_') and default()",
    documented_only=True,
    exclude_private=True,
    skip_empty_modules=True,
)

renderer = MarkdownRenderer(
    source_linker=GithubSourceLinker(root=".", repo="quixio/quix-streams"),
    source_position="after signature",
    source_format="[[VIEW SOURCE]]({url})",
    render_module_header=True,
    descriptive_class_title=False,
    header_level_by_type={
        "Module": 2,
        "Class": 3,
        "Method": 4,
        "Function": 4,
        "Variable": 4,
    },
)

session = PydocMarkdown(
    loaders=[loader],
    renderer=renderer,
    # processors=[filter_],
)
session.processors.append(filter_)
session.init(context)
modules = session.load_modules()


with open(f"{fp}/quixstreams.md", "w") as f:
    session.process(modules)
    f.write(session.renderer.render_to_string(modules))


doc_map = {
    "app": {
        "names": ["quixstreams.app"],
        "path": "application.md",
        "modules": [],
    },
    "topics_and_serdes": {
        "names": [
            "quixstreams.models.topics",
            "quixstreams.models.serializers.quix",
            "quixstreams.models.serializers.simple_types",
        ],
        "path": "topics-serdes.md",
        "modules": [],
    },
    "sdf": {
        "names": [
            "quixstreams.dataframe.dataframe",
            "quixstreams.dataframe.series",
            # note: additional filtering later for these below
            "quixstreams.state.types",
            "quixstreams.context",
        ],
        "path": "dataframe.md",
        "modules": [],
    },
    "Quix Platform API": {
        "names": [
            "quixstreams.platforms.quix.api",
            "quixstreams.platforms.quix.config",
            "quixstreams.platforms.quix.env",
        ],
        "path": "quix-platform-api.md",
        "modules": [],
    },
}

doc_modules = {_: k for k, maps in doc_map.items() for _ in maps["names"]}
for m in modules:
    if m.name in doc_modules:
        doc_map[doc_modules[m.name]]["modules"].append(m)

for m in doc_map["sdf"]["modules"]:
    if m.name == "quixstreams.context":
        m.members = [x for x in m.members if x.__class__.__name__ == "Function"]
    elif m.name == "quixstreams.state.types":
        m.members = [x for x in m.members if m.name == "State"]


for doc in doc_map.values():
    m = doc["modules"]
    with open(f"{fp}/{doc['path']}", "w") as f:
        session.process(m)
        f.write(session.renderer.render_to_string(m))
