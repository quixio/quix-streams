import collections
import copy
import functools
import itertools
from time import monotonic_ns
from typing import Any, Callable, List, Optional, Union

from typing_extensions import Self

from quixstreams.dataframe.exceptions import InvalidOperation

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
        self.children = set()
        self.generated = monotonic_ns()
        self.pruned = False

    def __repr__(self) -> str:
        """
        Generate a nice repr with all functions in the stream and its parents.

        :return: a string of format
            "<Stream [<total functions>]: <FuncType: func_name> | ... >"
        """
        tree_funcs = [s.func for s in self.root_path()]
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

    def diff(self, other: "Stream") -> Self:
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
        diff = self._diff_from_last_common_parent(other)

        # The following operations enforce direct splitting.
        # Enforcing direct splits relates to using one SDF to filter another.
        # Specifically there are various unintuitive cases, especially when using a
        # "split" SDF, where results will likely not be as expected, so we would
        # rather raise an exception instead.
        # See TestStreamingDataFrameSplitting test cases for examples.

        # the easiest check that catches most issues: the "inner" (filtering) sdf
        # should use same ref as the one being filtered; i.e. sdf[sdf.apply()].
        if diff[0].pruned:
            raise InvalidOperation(
                "Cannot use a filtering or column-setter SDF more than once"
            )

        elif diff[0] not in self.children:
            raise InvalidOperation(
                "filtering or column-setter SDF must originate from target SDF; "
                "ex: `sdf[sdf.apply()]`, NOT `sdf[other_sdf.apply()]` "
                "OR `sdf['x'] = sdf.apply()`, NOT `sdf['x'] = other_sdf.apply()`"
            )

        # With splitting there are some edge cases where the origin is the same,
        # but there are various side effects that can occur that we want to avoid.
        other_path_start = other.root_path(allow_splits=False)[0]
        if other_path_start.parent != diff[0].parent:
            # There is a split at the filtering SDF; still potentially valid
            if self.root_path(allow_splits=False)[0] != other_path_start:
                # This split is not shared by the filtered sdf
                raise InvalidOperation(
                    "filtering or column-setter SDF must originate from target SDF; "
                    "ex: `sdf[sdf.apply()]`, NOT `sdf[other_sdf.apply()]` "
                    "OR `sdf['x'] = sdf.apply()`, NOT `sdf['x'] = other_sdf.apply()`"
                )

        parent = None
        for node in diff:
            # Copy the node to ensure we don't alter the previously created Nodes
            node = copy.deepcopy(node)
            node.parent = parent
            parent = node
        self._prune(diff[0])
        return parent

    def root_path(self, allow_splits=True) -> List[Self]:
        """
        Return a list of all parent Streams including the node itself.

        Can optionally stop at a first encountered split with allow_splits=False

        The tree is ordered from parent to child (current node comes last).
        :return: a list of `Stream` objects
        """

        node = self
        tree_ = [node]
        while (parent := node.parent) and (allow_splits or len(parent.children) < 2):
            tree_.append(parent)
            node = node.parent

        # Reverse to get expected ordering.
        tree_.reverse()

        return tree_

    def full_tree(self) -> List[Self]:
        """
        Starts at tree root and finds every Stream in the tree (including splits).
        :return: The collection of all Streams interconnected to this one
        """
        return self._collect_nodes([], self.root_path()[0])

    def compose(
        self,
        allow_filters=True,
        allow_expands=True,
        allow_updates=True,
        allow_transforms=True,
        sink: Optional[Callable[[Any, Any, int, Any], None]] = None,
    ) -> VoidExecutor:
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

        composed = sink or self._default_sink
        composer = functools.partial(
            self._compose,
            allow_filters=allow_filters,
            allow_expands=allow_expands,
            allow_updates=allow_updates,
            allow_transforms=allow_transforms,
        )

        def _split_compose(pending_composes, composed, node):
            children = node.children

            if len(children) == 1:
                return _split_compose(pending_composes, composed, list(children)[0])

            for child in sorted(children, key=lambda node: node.generated):
                _split_compose(pending_composes, composed, child)
            tree = node.root_path(allow_splits=False)
            composed = composer(tree, pending_composes.pop(node, composed))

            if split := tree[0].parent:
                pending_composes.setdefault(split, []).append(composed)
            else:
                return composed

        return _split_compose({}, composed, self.full_tree()[0])

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

    def _compose(
        self,
        tree: List[Self],
        composed: List[Callable[[Any, Any, int, Any], None]],
        allow_filters: bool,
        allow_updates: bool,
        allow_expands: bool,
        allow_transforms: bool,
    ) -> VoidExecutor:
        functions = [node.func for node in tree]

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

            composed = func.get_executor(
                *composed if isinstance(composed, list) else [composed]
            )

        return composed

    def _diff_from_last_common_parent(self, other: Self) -> List[Self]:
        nodes_self = self.root_path()
        nodes_other = other.root_path()

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
        new_node = self.__class__(func=func, parent=self)
        self.children.add(new_node)
        return new_node

    def _default_sink(self, value: Any, key: Any, timestamp: int, headers: Any): ...

    def _prune(self, other: Self):
        """
        Removes a stream node by looking for where the "other" node is a child within
        the current and removing it.

        Note this means "other" must share a direct split point with this Stream.
        :param other: another Stream
        :return:
        """
        if other.pruned:
            raise ValueError("Node has already been pruned")
        other.pruned = True
        node = self
        while node:
            if other in node.children:
                node.children.remove(other)
                return
            node = node.parent
        raise ValueError("Node to prune is missing")

    def _collect_nodes(
        self, collected_nodes: List[Self], current_node: Self
    ) -> List[Self]:
        collected_nodes.append(current_node)
        for child in current_node.children:
            self._collect_nodes(collected_nodes, child)
        return collected_nodes
