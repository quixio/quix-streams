import pathlib

from pydoc_markdown import CrossrefProcessor, PydocMarkdown, SmartProcessor
from pydoc_markdown.contrib.loaders.python import PythonLoader
from pydoc_markdown.contrib.processors.filter import FilterProcessor
from pydoc_markdown.contrib.renderers.markdown import MarkdownRenderer
from pydoc_markdown.contrib.source_linkers.git import GithubSourceLinker
from pydoc_markdown.interfaces import Context

LIBRARY_ROOT_PATH = pathlib.Path("../..").resolve()
API_DOCS_PATH = pathlib.Path(LIBRARY_ROOT_PATH / "docs" / "api-reference").resolve()

# Load modules docs via pydoc_markdown
context = Context(
    directory=".",
)
loader = PythonLoader(
    search_path=[LIBRARY_ROOT_PATH.as_posix()], packages=["quixstreams"]
)
renderer = MarkdownRenderer(
    source_linker=GithubSourceLinker(
        root=LIBRARY_ROOT_PATH.as_posix(),
        repo="quixio/quix-streams",
        use_branch=True,
    ),
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
    processors=[
        FilterProcessor(),
        SmartProcessor(),
        CrossrefProcessor(),
        FilterProcessor(
            expression="not name.startswith('_') or name == '__init__' and default()",
            documented_only=True,
            exclude_private=True,
            skip_empty_modules=True,
        ),
    ],
)
session.init(context)
modules = session.load_modules()

# Write full API reference to a single file
FULL_API_REFERENCE_PATH = API_DOCS_PATH / "quixstreams.md"
with open(FULL_API_REFERENCE_PATH, "w") as f:
    print("Writing Full API Reference to", FULL_API_REFERENCE_PATH.as_posix())
    session.process(modules)
    f.write(session.renderer.render_to_string(modules))


# Map individual modules to doc files
doc_map = {
    "application.md": {
        k: None
        for k in [
            "quixstreams.app",
        ]
    },
    "state.md": {
        k: None
        for k in [
            "quixstreams.state.base.state",
            "quixstreams.state.rocksdb.options",
        ]
    },
    "serialization.md": {
        k: None
        for k in [
            "quixstreams.models.serializers.simple_types",
            "quixstreams.models.serializers.json",
            "quixstreams.models.serializers.avro",
            "quixstreams.models.serializers.protobuf",
            "quixstreams.models.serializers.schema_registry",
            "quixstreams.models.serializers.quix",
        ]
    },
    "kafka.md": {
        k: None for k in ["quixstreams.kafka.producer", "quixstreams.kafka.consumer"]
    },
    "topics.md": {
        "quixstreams.models.topics": None,
        "quixstreams.models.topics.admin": None,
        "quixstreams.models.topics.topic": None,
        "quixstreams.models.topics.manager": None,
        "quixstreams.models.topics.exceptions": None,
    },
    "dataframe.md": {
        k: None
        for k in [
            "quixstreams.dataframe.dataframe",
            "quixstreams.dataframe.series",
            "quixstreams.dataframe.joins.lookups.base",
            "quixstreams.dataframe.joins.lookups.sqlite",
            "quixstreams.dataframe.joins.lookups.postgresql",
            "quixstreams.dataframe.joins.join_asof",
        ]
    },
    "context.md": {
        k: None
        for k in [
            "quixstreams.context",
        ]
    },
    # Order: base, core, community
    "sinks.md": {
        k: None
        for k in [
            "quixstreams.sinks.base.sink",
            "quixstreams.sinks.base.batch",
            "quixstreams.sinks.base.exceptions",
            "quixstreams.sinks.core.influxdb3",
            "quixstreams.sinks.core.csv",
            "quixstreams.sinks.community.file.sink",
            "quixstreams.sinks.community.file.destinations.azure",
            "quixstreams.sinks.community.file.destinations.base",
            "quixstreams.sinks.community.file.destinations.local",
            "quixstreams.sinks.community.file.destinations.s3",
            "quixstreams.sinks.community.file.formats.base",
            "quixstreams.sinks.community.file.formats.json",
            "quixstreams.sinks.community.file.formats.parquet",
            "quixstreams.sinks.community.bigquery",
            "quixstreams.sinks.community.elasticsearch",
            "quixstreams.sinks.community.iceberg",
            "quixstreams.sinks.community.kinesis",
            "quixstreams.sinks.community.mongodb",
            "quixstreams.sinks.community.neo4j",
            "quixstreams.sinks.community.postgresql",
            "quixstreams.sinks.community.pubsub",
            "quixstreams.sinks.community.redis",
            "quixstreams.sinks.community.influxdb1",
            "quixstreams.sinks.community.tdengine.sink",
        ]
    },
    # Order: base, core, community
    "sources.md": {
        k: None
        for k in [
            "quixstreams.sources.base.source",
            "quixstreams.sources.core.csv",
            "quixstreams.sources.core.kafka.kafka",
            "quixstreams.sources.core.kafka.quix",
            "quixstreams.sources.community.file.azure",
            "quixstreams.sources.community.file.base",
            "quixstreams.sources.community.file.local",
            "quixstreams.sources.community.file.s3",
            "quixstreams.sources.community.file.compressions.gzip",
            "quixstreams.sources.community.file.formats.json",
            "quixstreams.sources.community.file.formats.parquet",
            "quixstreams.sources.community.kinesis.kinesis",
            "quixstreams.sources.community.mqtt",
            "quixstreams.sources.community.pubsub.pubsub",
            "quixstreams.sources.community.pandas",
            "quixstreams.sources.community.influxdb3.influxdb3",
        ]
    },
}

# Go over all modules and assign them to doc files
doc_modules = {name: f_name for f_name, m_name in doc_map.items() for name in m_name}
for m in modules:
    if m.name in doc_modules:
        doc_map[doc_modules[m.name]][m.name] = m

# Do some additional filtering within individual modules
for name, module in doc_map["state.md"].items():
    if name == "quixstreams.state.types":
        module.members = [x for x in module.members if x.name == "State"]

for name, module in doc_map["serialization.md"].items():
    if name == "quixstreams.models.serializers.quix":
        module.members = [x for x in module.members if x.name != "QuixSerializer"]

# Render the API docs for each module and write them to files
for doc_path, doc_modules in doc_map.items():
    modules_to_render = list(doc_modules.values())
    module_doc_path = API_DOCS_PATH / doc_path
    with open(module_doc_path, "w") as f:
        session.process(modules_to_render)
        s = session.renderer.render_to_string(modules_to_render)
        # Add some other nice formatting without polluting the docstrings
        s = s.replace("</a>\n\n####", "</a>\n\n<br><br>\n\n####")
        s = s.replace("**Arguments**:", "\n<br>\n***Arguments:***")
        s = s.replace("**Returns**:", "\n<br>\n***Returns:***")
        s = s.replace("Example Snippet:", "\n<br>\n***Example Snippet:***")
        s = s.replace("What it Does:", "\n<br>\n***What it Does:***")
        s = s.replace("How to Use:", "\n<br>\n***How to Use:***")
        print("Writing module API docs to", module_doc_path.as_posix())
        f.write(s)
