import collections
import copy
import itertools
from typing import List, Callable, Optional, Any, Union

from typing_extensions import Self

from .functions import (
    ApplyFunction,
    FilterFunction,
    UpdateFunction,
    StreamFunction,
    VoidExecutor,
    ReturningExecutor,
    FilterCallback,
    ApplyCallback,
    UpdateCallback,
    ApplyWithMetadataCallback,
    ApplyWithMetadataFunction,
    UpdateWithMetadataCallback,
    UpdateWithMetadataFunction,
    FilterWithMetadataCallback,
    FilterWithMetadataFunction,
    TransformCallback,
    TransformFunction,
    TransformExpandedCallback,
    ApplyWithMetadataExpandedCallback,
    ApplyExpandedCallback,
)

__all__ = ("Stream",)


class Stream:
    def __init__(
        self,
        func: Optional[StreamFunction] = None,
        parent: Optional[Self] = None,
    ):
        """
        A base class for all streaming operations.

        `Stream` is an abstraction of a function pipeline.
        Each Stream has a function and a parent (None by default).
        When adding new function to the stream, it creates a new `Stream` object and
        sets "parent" to the previous `Stream` to maintain an order of execution.

        Streams supports four types of functions:

        - "Apply" - generate new values based on a previous one.
            The result of an Apply function is passed downstream to the next functions.
            If "expand=True" is passed and the function returns an `Iterable`,
            each item of it will be treated as a separate value downstream.
        - "Update" - update values in-place.
            The result of an Update function is always ignored, and its input is passed
            downstream.
        - "Filter" - to filter values from the Stream.
            The result of a Filter function is interpreted as boolean.
            If it's `True`, the input will be passed downstream.
            If it's `False`, the record will be filtered from the stream.
        - "Transform" - to transform keys and timestamps along with the values.
            "Transform" functions may change the keys and should be used with caution.
            The result of the Transform function is passed downstream to the next
            functions.
            If "expand=True" is passed and the function returns an `Iterable`,
            each item of it will be treated as a separate value downstream.

        To execute the functions on the `Stream`, call `.compose()` method, and
        it will return a closure to execute all the functions accumulated in the Stream
        and its parents.

        :param func: a function to be called on the stream.
            It is expected to be wrapped into one of "Apply", "Filter", "Update" or
            "Trasform" from `quixstreams.core.stream.functions` package.
            Default - "ApplyFunction(lambda value: value)".
        :param parent: a parent `Stream`
        """
        if func is not None and not isinstance(func, StreamFunction):
            raise ValueError("Provided function must be a subclass of StreamFunction")

        self.func = func if func is not None else ApplyFunction(lambda value: value)
        self.parent = parent

    def __repr__(self) -> str:
        """
        Generate a nice repr with all functions in the stream and its parents.

        :return: a string of format
            "<Stream [<total functions>]: <FuncType: func_name> | ... >"
        """
        tree_funcs = [s.func for s in self.tree()]
        funcs_repr = " | ".join(
            (f"<{f.__class__.__name__}: {f.func.__qualname__}>" for f in tree_funcs)
        )
        return f"<{self.__class__.__name__} [{len(tree_funcs)}]: {funcs_repr}>"

    def add_filter(
        self,
        func: Union[FilterCallback, FilterWithMetadataCallback],
        *,
        metadata: bool = False,
    ) -> Self:
        """
        Add a function to filter values from the Stream.

        The return value of the function will be interpreted as `bool`.
        If the function returns `False`-like result, the Stream will raise `Filtered`
        exception during execution.

        :param func: a function to filter values from the stream
        :param metadata: if True, the callback will receive key and timestamp along with
            the value.
            Default - `False`.
        :return: a new `Stream` derived from the current one
        """
        if metadata:
            filter_func = FilterWithMetadataFunction(func)
        else:
            filter_func = FilterFunction(func)
        return self._add(filter_func)

    def add_apply(
        self,
        func: Union[
            ApplyCallback,
            ApplyExpandedCallback,
            ApplyWithMetadataCallback,
            ApplyWithMetadataExpandedCallback,
        ],
        *,
        expand: bool = False,
        metadata: bool = False,
    ) -> Self:
        """
        Add an "apply" function to the Stream.

        The function is supposed to return a new value, which will be passed
        further during execution.

        :param func: a function to generate a new value
        :param expand: if True, expand the returned iterable into individual values
            downstream. If returned value is not iterable, `TypeError` will be raised.
            Default - `False`.
        :param metadata: if True, the callback will receive key and timestamp along with
            the value.
            Default - `False`.
        :return: a new `Stream` derived from the current one
        """
        if metadata:
            apply_func = ApplyWithMetadataFunction(func, expand=expand)
        else:
            apply_func = ApplyFunction(func, expand=expand)
        return self._add(apply_func)

    def add_update(
        self,
        func: Union[UpdateCallback, UpdateWithMetadataCallback],
        *,
        metadata: bool = False,
    ) -> Self:
        """
        Add an "update" function to the Stream, that will mutate the input value.

        The return of this function will be ignored and its input
        will be passed downstream.

        :param func: a function to mutate the value
        :param metadata: if True, the callback will receive key and timestamp along with
            the value.
            Default - `False`.
        :return: a new Stream derived from the current one
        """
        if metadata:
            update_func = UpdateWithMetadataFunction(func)
        else:
            update_func = UpdateFunction(func)
        return self._add(update_func)

    def add_transform(
        self,
        func: Union[TransformCallback, TransformExpandedCallback],
        *,
        expand: bool = False,
    ) -> Self:
        """
        Add a "transform" function to the Stream, that will mutate the input value.

        The callback must accept a value, a key, and a timestamp.
        It's expected to return a new value, new key and new timestamp.

        The result of the callback which will be passed downstream
        during execution.


        :param func: a function to mutate the value
        :param expand: if True, expand the returned iterable into individual items
            downstream. If returned value is not iterable, `TypeError` will be raised.
            Default - `False`.
        :return: a new Stream derived from the current one
        """

        return self._add(TransformFunction(func, expand=expand))

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

        The tree is ordered from parent to child (current node comes last).
        :return: a list of `Stream` objects
        """

        tree_ = [self]
        node = self
        while node.parent:
            tree_.append(node.parent)
            node = node.parent

        # Reverse to get expected ordering.
        tree_.reverse()

        return tree_

    def compose_returning(self) -> ReturningExecutor:
        """
        Compose a list of functions from this `Stream` and its parents into one
        big closure that always returns the transformed record.

        This closure is to be used to execute the functions in the stream and to get
        the result of the transformations.

        Stream may only contain simple "apply" functions to be able to compose itself
        into a returning function.
        """
        # Sink results of the Stream to a single-item queue, and read from this queue
        # after executing the Stream.
        # The composed stream must have only the "apply" functions,
        # which always return a single.
        buffer = collections.deque(maxlen=1)
        composed = self.compose(
            allow_filters=False,
            allow_expands=False,
            allow_updates=False,
            allow_transforms=False,
            sink=lambda value, key, timestamp, headers: buffer.appendleft(
                (value, key, timestamp, headers)
            ),
        )

        def wrapper(value: Any, key: Any, timestamp: int, headers: Any) -> Any:
            try:
                # Execute the stream and return the result from the queue
                composed(value, key, timestamp, headers)
                return buffer.popleft()
            finally:
                # Always clean the queue after the Stream is executed
                buffer.clear()

        return wrapper

    def compose(
        self,
        allow_filters: bool = True,
        allow_updates: bool = True,
        allow_expands: bool = True,
        allow_transforms: bool = True,
        sink: Optional[Callable[[Any, Any, int, Any], None]] = None,
    ) -> VoidExecutor:
        """
        Compose a list of functions from this `Stream` and its parents into one
        big closure using a "composer" function.

        This "executor" closure is to be used to execute all functions in the stream for the given
        key, value and timestamps.

        By default, executor doesn't return the result of the execution.
        To accumulate the results, pass the `sink` parameter.

        :param allow_filters: If False, this function will fail with `ValueError` if
            the stream has filter functions in the tree. Default - True.
        :param allow_updates: If False, this function will fail with `ValueError` if
            the stream has update functions in the tree. Default - True.
        :param allow_expands: If False, this function will fail with `ValueError` if
            the stream has functions with "expand=True" in the tree. Default - True.
        :param allow_transforms: If False, this function will fail with `ValueError` if
            the stream has transform functions in the tree. Default - True.
        :param sink: callable to accumulate the results of the execution, optional.

        :raises ValueError: if disallowed functions are present in the stream tree.
        """

        tree = self.tree()
        functions = [node.func for node in tree]

        composed = sink or self._default_sink

        # Iterate over a reversed list of functions
        for func in reversed(functions):
            # Validate that only allowed functions are passed
            if not allow_updates and isinstance(
                func, (UpdateFunction, UpdateWithMetadataFunction)
            ):
                raise ValueError("Update functions are not allowed")
            elif not allow_filters and isinstance(
                func, (FilterFunction, FilterWithMetadataFunction)
            ):
                raise ValueError("Filter functions are not allowed")
            elif not allow_transforms and isinstance(func, TransformFunction):
                raise ValueError("Transform functions are not allowed")
            elif not allow_expands and func.expand:
                raise ValueError("Expand functions are not allowed")

            # Compose functions from the tree together so the top function calls
            # the bottom one
            composed = func.get_executor(composed)

        return composed

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

    def _add(self, func: StreamFunction) -> Self:
        return self.__class__(func=func, parent=self)

    def _default_sink(self, value: Any, key: Any, timestamp: int, headers: Any): ...
