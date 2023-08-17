import logging
import uuid
from copy import deepcopy
from typing import Self, Optional, Callable, Any, Union

from ..models import Row

logger = logging.getLogger(__name__)

__all__ = (
    "InvalidPipelineBranching",
    "PipelineFunction",
    "Pipeline",
    "get_func_name"
)


def get_attrs(obj, attrs):
    attrs = attrs.split('.')
    for a in attrs:
        obj = obj.__getattribute__(a)
    return obj


def get_func_name(func):
    for item in ['__name__', 'func.__name__', '__class__.__name__']:
        try:
            return get_attrs(func, item)
        except AttributeError:
            pass
    return "callable"


class InvalidPipelineBranching(Exception):
    pass


# TODO: Docstrings
class PipelineFunction:
    def __init__(self, func: Union[Callable], pipeline_name: str, name: str = None):
        self._id = str(uuid.uuid4())
        self._pipeline_name = pipeline_name
        self._func = func
        self._name = name or get_func_name(self._func)
        logger.debug(f'Generated PipelineFunction {self.full_id}')

    def __call__(self, row: Row):
        return self._func(row)

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def pipeline_name(self) -> str:
        return self._pipeline_name

    @property
    def full_id(self):
        return f"[P:{self.pipeline_name}];[F:{self.name}];[ID:{self.id}]"


def process_function(
        func: PipelineFunction, data: Any
) -> Optional[Union[Any, list[Any]]]:
    if isinstance(data, list):
        return [fd for fd in (func(d) for d in data) if fd is not None] or None
    else:
        return func(data)


class Pipeline:
    def __init__(
            self, name: str = None, functions: list[PipelineFunction] = None,
            graph: dict = None, parent: str = None
    ):
        self._functions = functions or []
        self._name = name or str(uuid.uuid4())
        self._graph = graph or {}
        self._parent = parent
        self._add_graph_node()

    @property
    def functions(self) -> list[PipelineFunction]:
        return self._functions

    @property
    def name(self) -> str:
        return self._name

    @property
    def graph(self) -> dict:
        return self._graph

    @property
    def parent(self) -> str:
        return self._parent

    def clone(self) -> Self:
        return self.__class__(
            functions=self._functions[:], graph=deepcopy(self.graph), parent=self.name
        )

    def _add_graph_node(self):
        if self.parent:
            self._graph[self.parent] = self.name
        else:
            self._graph[self.name] = None

    def _check_child_contains_parent(self, parent: Self):
        for key in parent.graph:
            if parent_child := parent.graph[key]:
                try:
                    assert self.graph[key] == parent_child
                except AssertionError:
                    raise InvalidPipelineBranching

    def remove_redundant_functions(self, parent: Self) -> Self:
        self._check_child_contains_parent(parent)
        self._functions = self.functions[
                          -(len(self.functions) - len(parent.functions)):
                          ]
        return self

    def apply(self, func: Callable, func_name: str = None) -> Self:
        self.functions.append(
            PipelineFunction(func=func, pipeline_name=self.name, name=func_name)
        )
        return self

    def process(self, data: Any) -> Optional[Any]:
        result = data
        for func in self.functions:
            logger.debug(
                f'Pipeline {self.name} processing func {func.name} with value {result}')
            result = process_function(func, result)
            logger.debug(f'Pipeline {self.name} result of {func.name} is: {result}')
            if result is None:
                logger.info(
                    f'Pipeline {self.name} processing step returned a None; \
                    terminating processing')
                break
        logger.debug(f'Pipeline {self.name} finished; result is {result}')
        return result
