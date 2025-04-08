import collections
import copy
from collections import deque
from graphlib import TopologicalSorter
from typing import (
    Any,
    Deque,
    List,
    Literal,
    Optional,
    Union,
    cast,
    overload,
)

from quixstreams.dataframe.exceptions import InvalidOperation

from .exceptions import InvalidTopology
from .functions import (
    ApplyCallback,
    ApplyExpandedCallback,
    ApplyFunction,
    ApplyWithMetadataCallback,
    ApplyWithMetadataExpandedCallback,
    ApplyWithMetadataFunction,
    FilterCallback,
    FilterFunction,
    FilterWithMetadataCallback,
    FilterWithMetadataFunction,
    ReturningExecutor,
    StreamFunction,
    TransformCallback,
    TransformExpandedCallback,
    TransformFunction,
    UpdateCallback,
    UpdateFunction,
    UpdateWithMetadataCallback,
    UpdateWithMetadataFunction,
    VoidExecutor,
)

__all__ = ("Stream", "InvalidTopology")


class Stream:
    def __init__(
        self,
        func: Optional[StreamFunction] = None,
        parents: Optional[List["Stream"]] = None,
    ):
        """
        A base class for all streaming operations.

        `Stream` is an abstraction of a function pipeline.
        Each Stream has a function and an optional list of parents (None by default).
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
        :param parents: an optional list of parent `Stream`s
        """
        if func is not None and not isinstance(func, StreamFunction):
            raise ValueError("Provided function must be a subclass of StreamFunction")

        self.func = func if func is not None else ApplyFunction(lambda value: value)
        self.parents = parents or []
        self.children: list["Stream"] = []
        self.pruned = False

    def __repr__(self) -> str:
        """
        Generate a nice repr with all functions in the stream and its parents.

        :return: a string of format
            "<Stream [<total functions>]: <FuncType: func_name> | ... >"
        """
        func_repr = f"<{self.func.__class__.__name__}: {self.func.func.__qualname__}>"
        return (
            f"<{self.__class__.__name__} "
            f"(parents={len(self.parents)} children={len(self.children)}): "
            f"{func_repr}>"
        )

    @overload
    def add_filter(self, func: FilterCallback, *, metadata: Literal[False] = False):
        pass

    @overload
    def add_filter(self, func: FilterWithMetadataCallback, *, metadata: Literal[True]):
        pass

    def add_filter(
        self,
        func: Union[FilterCallback, FilterWithMetadataCallback],
        *,
        metadata: bool = False,
    ) -> "Stream":
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
            filter_func: StreamFunction = FilterWithMetadataFunction(
                cast(FilterWithMetadataCallback, func)
            )
        else:
            filter_func = FilterFunction(cast(FilterCallback, func))
        return self._add(filter_func)

    @overload
    def add_apply(
        self,
        func: ApplyCallback,
        *,
        expand: Literal[False] = False,
        metadata: Literal[False] = False,
    ):
        pass

    @overload
    def add_apply(
        self,
        func: ApplyExpandedCallback,
        *,
        expand: Literal[True],
        metadata: Literal[False] = False,
    ):
        pass

    @overload
    def add_apply(
        self,
        func: ApplyWithMetadataCallback,
        *,
        expand: Literal[False] = False,
        metadata: Literal[True],
    ):
        pass

    @overload
    def add_apply(
        self,
        func: ApplyWithMetadataExpandedCallback,
        *,
        expand: Literal[True],
        metadata: Literal[True],
    ):
        pass

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
    ) -> "Stream":
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
            apply_func: StreamFunction = ApplyWithMetadataFunction(func, expand=expand)  # type: ignore[call-overload]
        else:
            apply_func = ApplyFunction(func, expand=expand)  # type: ignore[call-overload]
        return self._add(apply_func)

    @overload
    def add_update(self, func: UpdateCallback, *, metadata: Literal[False] = False):
        pass

    @overload
    def add_update(self, func: UpdateWithMetadataCallback, *, metadata: Literal[True]):
        pass

    def add_update(
        self,
        func: Union[UpdateCallback, UpdateWithMetadataCallback],
        *,
        metadata: bool = False,
    ) -> "Stream":
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
            update_func: StreamFunction = UpdateWithMetadataFunction(
                cast(UpdateWithMetadataCallback, func)
            )
        else:
            update_func = UpdateFunction(cast(UpdateCallback, func))
        return self._add(update_func)

    @overload
    def add_transform(self, func: TransformCallback, *, expand: Literal[False] = False):
        pass

    @overload
    def add_transform(self, func: TransformExpandedCallback, *, expand: Literal[True]):
        pass

    def add_transform(
        self,
        func: Union[TransformCallback, TransformExpandedCallback],
        *,
        expand: bool = False,
    ) -> "Stream":
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
        return self._add(TransformFunction(func, expand=expand))  # type: ignore[call-overload]

    def merge(self, other: "Stream") -> "Stream":
        """
        Merge two Streams together and return a new Stream with two parents

        :param other: a `Stream` to merge with.
        """

        # Check if other is not already present in the "self" Stream topology.
        # Otherwise, it will create a cycle in the graph breaking the DAG
        if other is self:
            # Merging a stream with itself is prohibited
            raise InvalidOperation("Cannot merge a SDF with itself")
        elif other in self.root_path() or self in other.root_path():
            # Merge child streams with parents and vice versa is prohibited
            raise InvalidOperation("The target SDF is already present in the topology")

        merged_stream = self.__class__(parents=[self, other])
        self.children.append(merged_stream)
        other.children.append(merged_stream)
        return merged_stream

    def diff(self, other: "Stream") -> "Stream":
        """
        Takes the difference between Streams `self` and `other` based on their last
        common parent, and returns a new, independent `Stream` that includes only
        this difference (the start of the "diff" will have no parent).

        It's impossible to calculate a diff when:
         - Streams don't have a common parent.
         - When the `self` Stream already includes all the nodes from
            the `other` Stream, and the resulting diff is empty.

        :param other: a `Stream` to take a diff from.
        :raises ValueError: if Streams don't have a common parent,
            if the diff is empty, or pruning failed.
        :return: a new independent `Stream` instance whose root begins at the diff
        """
        # Traverse the children of "self" and look for the "other" Stream among then
        diff: Deque["Stream"] = deque()
        self_found = False

        for stream in other.root_path():
            if stream is self:
                self_found = True
                break
            # Assigning or filtering using branched SDFs
            # may lead to an undefined behavior
            if stream.is_branched():
                raise InvalidOperation("Cannot assign or filter using a branched SDF")
            elif stream.is_merged():
                # Assigning or filtering using merged SDFs may lead
                # to an undefined behavior too
                raise InvalidOperation(
                    "Cannot assign or filter using the SDF merged with another SDF"
                )

            diff.appendleft(stream)

        if not diff:
            # The case of "sdf['x'] = sdf" or "sdf = sdf[sdf]"
            raise InvalidOperation("Cannot assign or filter using the same SDF")

        # Reverse the diff to be in "parent->children" order
        diff_head, *diff_rest = diff

        if diff_head.pruned:
            raise InvalidOperation(
                "Cannot use a filtering or column-setter SDF more than once"
            )
        if not self_found:
            # "self" is not found among the parents of "other"
            raise InvalidOperation(
                "filtering or column-setter SDF must originate from target SDF; "
                "ex: `sdf[sdf.apply()]`, NOT `sdf[other_sdf.apply()]` "
                "OR `sdf['x'] = sdf.apply()`, NOT `sdf['x'] = other_sdf.apply()`"
            )

        # Cut off the diffed nodes from the rest of the tree
        # by removing the diff head node from the current node's children
        diff_head.pruned = True
        self.children.remove(diff_head)
        diff_head.parents = []

        # Copy the diff head and all nodes related to it
        # to ensure the diff is fully extracted from the tree
        # and not connected to other nodes.
        diff_head = copy.deepcopy(diff_head)
        parents = [diff_head]
        for stream in diff_rest:
            stream = copy.deepcopy(stream)
            stream.parents = parents
            stream.children = copy.deepcopy(stream.children)
            parents = [stream]
        return diff_head

    def full_tree(self) -> List["Stream"]:
        """
        Find every related Stream in the tree across all children and parents and return
        them in a topologically sorted order.
        """
        # Sort the tree in the topological order using graphlib.TopologicalSorter
        # Topological sorting resolves all dependencies in the graph, and child nodes
        # always come after all the parents
        sorter: TopologicalSorter = TopologicalSorter()
        visited: set["Stream"] = set()

        # Add all the nodes to the TopologicalSorter
        to_traverse: Deque["Stream"] = collections.deque()
        to_traverse.append(self)
        while to_traverse:
            node = to_traverse.popleft()
            if node in visited:
                continue
            sorter.add(node, *node.parents)
            visited.add(node)
            to_traverse += node.parents + node.children

        # Sort the nodes in the topological order
        nodes = list(sorter.static_order())
        return nodes

    def compose(
        self,
        allow_filters=True,
        allow_expands=True,
        allow_updates=True,
        allow_transforms=True,
        sink: Optional[VoidExecutor] = None,
    ) -> dict["Stream", VoidExecutor]:
        """
        Generate an "executor" closure by mapping all relatives of this `Stream` and
        composing their functions together.

        The resulting "executor" can be called with a given
        value, key, timestamp, and headers (i.e. a Kafka message).

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

        """
        sink = sink or self._default_sink
        executors: dict["Stream", VoidExecutor] = {}

        for stream in reversed(self.full_tree()):
            func = stream.func

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

            if stream.children:
                child_executors = [executors[child] for child in stream.children]
            else:
                child_executors = [sink]

            executor = func.get_executor(*child_executors)
            executors[stream] = executor

        root_executors = {s: e for s, e in executors.items() if not s.parents}
        return root_executors

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
        # The composed stream must consist of the "apply" functions
        # that always return a single result.

        buffer: Deque[tuple[Any, Any, int, Any]] = collections.deque(maxlen=1)
        executor = self.compose_single(
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
                executor(value, key, timestamp, headers)
                return buffer.popleft()
            finally:
                # Always clean the queue after the Stream is executed
                buffer.clear()

        return wrapper

    def compose_single(
        self,
        allow_filters=True,
        allow_expands=True,
        allow_updates=True,
        allow_transforms=True,
        sink: Optional[VoidExecutor] = None,
    ) -> VoidExecutor:
        """
        A helper function to compose a Stream with a single root.
        If there's more than one root in the topology,
        it will fail with the `InvalidTopology` error.


        :param allow_filters: If False, this function will fail with `ValueError` if
            the stream has filter functions in the tree. Default - True.
        :param allow_updates: If False, this function will fail with `ValueError` if
            the stream has update functions in the tree. Default - True.
        :param allow_expands: If False, this function will fail with `ValueError` if
            the stream has functions with "expand=True" in the tree. Default - True.
        :param allow_transforms: If False, this function will fail with `ValueError` if
            the stream has transform functions in the tree. Default - True.
        :param sink: callable to accumulate the results of the execution, optional.
        """
        composed = self.compose(
            allow_filters=allow_filters,
            allow_expands=allow_expands,
            allow_updates=allow_updates,
            allow_transforms=allow_transforms,
            sink=sink,
        )
        if len(composed) > 1:
            raise InvalidTopology(f"Expected a single root Stream, got {len(composed)}")
        return list(composed.values())[0]

    def is_merged(self) -> bool:
        return len(self.parents) > 1

    def is_branched(self) -> bool:
        return len(self.children) > 1

    def root_path(self) -> List["Stream"]:
        """
        Start from self and collect all parents until reaching the root nodes
        """
        nodes = [self]
        to_traverse: Deque = collections.deque([self])
        while to_traverse:
            node = to_traverse.popleft()
            for parent in node.parents:
                nodes.append(parent)
                to_traverse.append(parent)
        return nodes

    def _add(self, func: StreamFunction) -> "Stream":
        new_node = self.__class__(func=func, parents=[self])
        self.children.append(new_node)
        return new_node

    def _default_sink(
        self, value: Any, key: Any, timestamp: int, headers: Any
    ) -> None: ...
