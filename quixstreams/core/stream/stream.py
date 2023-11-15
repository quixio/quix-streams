import copy
import functools
import itertools
from typing import Any, List, Callable, Optional, TypeVar

from typing_extensions import Self

from .functions import *

__all__ = ("Stream",)

R = TypeVar("R")
T = TypeVar("T")


class Stream:
    def __init__(
        self,
        func: Optional[Callable[[Any], Any]] = None,
        parent: Optional[Self] = None,
    ):
        """
        A base class for all streaming operations.

        `Stream` is an abstraction of a function pipeline.
        Each Stream has a function and a parent (None by default).
        When adding new function to the stream, it creates a new `Stream` object and
        sets "parent" to the previous `Stream` to maintain an order of execution.

        Streams supports 3 types of functions:
        - "Apply" - generate new values based on a previous one.
            The result of an Apply function is passed downstream to the next functions.
        - "Update" - update values in-place.
            The result of an Update function is always ignored, and its input is passed
            downstream.
        - "Filter" - to filter values from the Stream.
            The result of a Filter function is interpreted as boolean.
            If it's `True`, the input will be passed downstream.
            If it's `False`, the `Filtered` exception will be raised to signal that the
            value is filtered out.

        To execute functions accumulated on the `Stream` instance, it must be compiled
        using `.compile()` method, which returns a function closure executing all the
        functions in the tree.

        :param func: a function to be called on the stream.
            It is expected to be wrapped into one of "Apply", "Filter" or "Update" from
            `quixstreams.core.stream.functions` package.
            Default - "Apply(lambda v: v)".
        :param parent: a parent `Stream`
        """
        if func is not None and not any(
            (
                is_apply_function(func),
                is_update_function(func),
                is_filter_function(func),
            )
        ):
            raise ValueError(
                "Provided function must be either Apply, Filter or Update function"
            )

        self.func = func if func is not None else Apply(lambda x: x)
        self.parent = parent

    def __repr__(self) -> str:
        """
        Generate a nice repr with all functions in the stream and its parents.

        :return: a string of format
            "<Stream [<total functions>]: <FuncType: func_name> | ... >"
        """
        tree_funcs = [s.func for s in self.tree()]
        funcs_repr = " | ".join(
            (
                f"<{get_stream_function_type(f).name}: {f.__qualname__}>"
                for f in tree_funcs
            )
        )
        return f"<{self.__class__.__name__} [{len(tree_funcs)}]: {funcs_repr}>"

    def add_filter(self, func: Callable[[T], R]) -> Self:
        """
        Add a function to filter values from the Stream.

        The return value of the function will be interpreted as `bool`.
        If the function returns `False`-like result, the Stream will raise `Filtered`
        exception during execution.

        :param func: a function to filter values from the stream
        :return: a new `Stream` derived from the current one
        """
        return self._add(Filter(func))

    def add_apply(self, func: Callable[[T], R]) -> Self:
        """
        Add an "apply" function to the Stream.

        The function is supposed to return a new value, which will be passed
        further during execution.

        :param func: a function to generate a new value
        :return: a new `Stream` derived from the current one
        """
        return self._add(Apply(func))

    def add_update(self, func: Callable[[T], object]) -> Self:
        """
        Add an "update" function to the Stream, that will mutate the input value.

        The return of this function will be ignored and its input
        will be passed downstream.

        :param func: a function to mutate the value
        :return: a new Stream derived from the current one
        """
        return self._add((Update(func)))

    def diff(
        self,
        other: "Stream",
    ) -> Self:
        """
        Takes the difference between Streams `self` and `other` based on their last
        common parent, and returns a new `Stream` that includes only this difference.

        It's impossible to calculate a diff when:
         - Streams don't have a common parent.
         - When the `self` Stream already includes all the nodes from
            the `other` Stream, and the resulting diff is empty.

        :param other: a `Stream` to take a diff from.
        :raises ValueError: if Streams don't have a common parent
            or if the diff is empty.
        :return: new `Stream` instance including all the Streams from the diff
        """
        diff = self._diff_from_last_common_parent(other)
        parent = None
        head = None
        for node in diff:
            # Copy the node to ensure we don't alter the previously created Nodes
            node = copy.deepcopy(node)
            node.parent = parent
            parent = node
            head = node
        return head

    def tree(self) -> List[Self]:
        """
        Return a list of all parent Streams including the node itself.

        The tree is ordered from child to parent (current node comes first).
        :return: a list of `Stream` objects
        """
        tree_ = [self]
        node = self
        while node.parent:
            tree_.insert(0, node.parent)
            node = node.parent
        return tree_

    def compile(
        self,
        allow_filters: bool = True,
        allow_updates: bool = True,
    ) -> Callable[[T], R]:
        """
        Compile a list of functions from this `Stream` and its parents into a single
        big closure using a "compiler" function.

        Closures are more performant than calling all the functions in the
        `Stream.tree()` one-by-one.

        :param allow_filters: If False, this function will fail with ValueError if
            the stream has filter functions in the tree. Default - True.
        :param allow_updates: If False, this function will fail with ValueError if
            the stream has update functions in the tree. Default - True.

        :raises ValueError: if disallowed functions are present in the stream tree.
        """
        compiled = None
        tree = self.tree()
        for node in tree:
            func = node.func
            if not allow_updates and is_update_function(func):
                raise ValueError("Update functions are not allowed in this stream")
            elif not allow_filters and is_filter_function(func):
                raise ValueError("Filter functions are not allowed in this stream")

            if compiled is None:
                compiled = func
            else:
                compiled = compiler(func)(compiled)

        return compiled

    def _diff_from_last_common_parent(self, other: Self) -> List[Self]:
        nodes_self = self.tree()
        nodes_other = other.tree()

        diff = []
        last_common_parent = None
        for node_self, node_other in itertools.zip_longest(nodes_self, nodes_other):
            if node_self is node_other:
                last_common_parent = node_other
            elif node_other is not None:
                diff.append(node_other)

        if last_common_parent is None:
            raise ValueError("Common parent not found")
        if not diff:
            raise ValueError("The diff is empty")
        return diff

    def _add(self, func: Callable[[T], R]) -> Self:
        return self.__class__(func=func, parent=self)


def compiler(outer_func: Callable[[T], R]) -> Callable[[T], R]:
    """
    A function that wraps two other functions into a closure.
    It passes the result of thje inner function as an input to the outer function.

    It is used to transform (aka "compile") a list of functions into one large closure
    like this:
    ```
    [func, func, func] -> func(func(func()))
    ```

    :return: a function with one argument (value)
    """

    def wrapper(inner_func: Callable[[T], R]):
        def _wrapper(v: T) -> R:
            return outer_func(inner_func(v))

        return functools.update_wrapper(_wrapper, inner_func)

    return functools.update_wrapper(wrapper, outer_func)
