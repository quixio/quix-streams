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


filter_ = FilterProcessor(
    expression="not name.startswith('_') or name == '__init__' and default()",
    documented_only=True,
    exclude_private=True,
    skip_empty_modules=True,
)

renderer = MarkdownRenderer(
    source_linker=GithubSourceLinker(root="../..", repo="quixio/quix-streams"),
    source_position="after signature",
    source_format="[[VIEW SOURCE]]({url})",
    descriptive_class_title=False,
    add_method_class_prefix=True,
    render_module_header=True,
    header_level_by_type={
        "Module": 2,  # >1 level 1 header breaks things
        "Class": 3,
        "Method": 4,
        "Function": 4,
        "Variable": 4,
    },
)

session = PydocMarkdown(
    loaders=[loader],
    renderer=renderer,
)
session.processors.append(filter_)
session.init(context)
modules = session.load_modules()


with open(f"{fp}/quixstreams.md", "w") as f:
    session.process(modules)
    f.write(session.renderer.render_to_string(modules))


doc_map = {
    "application.md": {
        k: None
        for k in [
            "quixstreams.app",
            "quixstreams.state.types",
        ]
    },
    "topics-serdes.md": {
        k: None
        for k in [
            "quixstreams.models.topics",
            "quixstreams.models.serializers.quix",
            "quixstreams.models.serializers.simple_types",
        ]
    },
    "dataframe.md": {
        k: None
        for k in [
            "quixstreams.dataframe.dataframe",
            "quixstreams.dataframe.series",
            # note: additional filtering later for these below
            "quixstreams.context",
        ]
    },
    "quix-platform-api.md": {
        k: None
        for k in [
            "quixstreams.platforms.quix.api",
            "quixstreams.platforms.quix.config",
            "quixstreams.platforms.quix.env",
        ]
    },
}

doc_modules = {name: f_name for f_name, m_name in doc_map.items() for name in m_name}
for m in modules:
    if m.name in doc_modules:
        doc_map[doc_modules[m.name]][m.name] = m

for name, module in doc_map["application.md"].items():
    if name == "quixstreams.state.types":
        module.members = [x for x in module.members if x.name == "State"]

for name, module in doc_map["topics-serdes.md"].items():
    if name == "quixstreams.models.serializers.quix":
        module.members = [x for x in module.members if x.name != "QuixSerializer"]

for name, module in doc_map["dataframe.md"].items():
    if name == "quixstreams.context":
        module = [x for x in module.members if x.__class__.__name__ == "Function"]


for doc_path, modules in doc_map.items():
    m = list(modules.values())
    with open(f"{fp}/{doc_path}", "w") as f:
        session.process(m)
        s = session.renderer.render_to_string(m)
        # Add some other nice formatting without polluting the docstrings
        s = s.replace("</a>\n\n####", "</a>\n\n<br><br>\n\n####")
        s = s.replace("**Arguments**:", "\n<br>\n***Arguments:***")
        s = s.replace("**Returns**:", "\n<br>\n***Returns:***")
        s = s.replace("Example Snippet:", "\n<br>\n***Example Snippet:***")
        s = s.replace("What it Does:", "\n<br>\n***What it Does:***")
        s = s.replace("How to Use:", "\n<br>\n***How to Use:***")
        f.write(s)
