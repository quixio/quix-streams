import logging
import uuid
from typing import Self, Optional, Callable, Any, Union

from ..models import Row

logger = logging.getLogger(__name__)

__all__ = ("PipelineFunction", "Pipeline")


class PipelineFunction:
    def __init__(self, func: Union[Callable]):
        self._id = str(uuid.uuid4())
        self._func = func

    @property
    def id(self) -> str:
        return self._id

    def __repr__(self):
        return f'<{self.__class__.__name__} "{repr(self._func)}">'

    def __call__(self, row: Row):
        return self._func(row)


class Pipeline:
    def __init__(
        self,
        functions: list[PipelineFunction] = None,
        _id: str = None
    ):
        self._id = _id or str(uuid.uuid4())
        self._functions = functions or []

    @property
    def functions(self) -> list[PipelineFunction]:
        return self._functions

    @property
    def id(self) -> str:
        return self._id

    def apply(self, func: Callable) -> Self:
        """
        Add a callable to the Pipeline execution list.
        The provided callable should accept a single argument, which will be its input.
        The provided callable should similarly return one output, or None

        Note that if the expected input is a list, this function will be called with
        each element in that list rather than the list itself.

        :param func: callable that accepts and (usually) returns an object
        :return: self (Pipeline)
        """
        self.functions.append(PipelineFunction(func=func))
        return self

    def process(self, data: Any) -> Optional[Any]:
        """
        Execute the previously defined Pipeline functions on a provided input, `data`.

        Note that if `data` is a list, each function will be called with each element
        in that list rather than the list itself.

        :param data: any object, but usually a QuixStreams Row
        :return: an object OR list of objects OR None (if filtered)
        """
        # TODO: maybe have an arg that allows passing "blacklisted" result types
        # or SDF inspects each result somehow?
        result = data
        for func in self.functions:
            if isinstance(result, list):
                result = [
                    fd for fd in (func(d) for d in result) if fd is not None
                ] or None
            else:
                result = func(result)
            if result is None:
                logger.debug(
                    "Pipeline {pid} processing step returned a None; "
                    "terminating processing".format(pid=self.id)
                )
                break
        return result
