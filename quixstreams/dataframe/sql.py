import logging
import math
from datetime import timedelta, datetime
from functools import singledispatchmethod
from typing import Dict

from sqlglot import exp, planner, optimizer
from sqlglot.errors import ExecuteError
from sqlglot.executor.env import ENV
from sqlglot.executor.python import Python
from sqlglot.expressions import Table, Alias, Column
from sqlglot.optimizer import build_scope

from sqlglot.planner import Plan
from sqlglot import parse_one

logger = logging.getLogger(__name__)

# TODO: create a new SQL dialect instead of hacking these in
# Potentially Apache Calcite?
WINDOW_TYPES = {
    "TUMBLE": "tumbling_window",
    "HOP": "hopping_window",
}


class StreamingSQLExecutor:
    """
    Very simple start means:
    - only one dataframe & stream supported - this means no joins
    - no state - this means no windows or aggregations
    """  

    def __init__(self, query: str, env=None):
        self.query = query
        self.node = parse_one(query)
        self.generator = Python().generator(identify=True, comments=False)
        self.env = {**ENV, **(env or {}), "COUNT": _sql_count, "scope": {}}

    def add_to_stream(self, tables: Dict, table_schemas: Dict[str, Dict] = None):
        # Slightly long-winded so that we don't include CTEs here
        required_tables = set(
            source.name or source.alias
            for scope in build_scope(self.node).traverse()
            for alias, (node, source) in scope.selected_sources.items()
            if isinstance(source, exp.Table)
        )
        if not required_tables.issubset(tables):
            raise ValueError(f"Missing tables: {required_tables - set(tables)}")

        node = self.node
        if table_schemas:
            node = optimizer.optimize(self.node, schema=table_schemas, leave_tables_isolated=True)
        elif not required_tables.issubset(table_schemas or {}):
            logger.warning(f"Some table schemas missing {required_tables - set(tables)}. "
                           f"Not all SQL operations can be optimised or validated ahead of time.")
            # But we still apply this as it makes the plan execution simpler
            node = optimizer.optimize(self.node, leave_tables_isolated=True)

        return self._execute(plan=Plan(node), tables={t: tables[t] for t in required_tables})

    def _execute(self, plan, tables):
        finished = set()
        queue = set(plan.leaves)
        # contexts is a mapping from a node to the result of executing that node
        # the result of executing a node is a table
        contexts = {**tables}

        while queue:
            node = queue.pop()
            print(node)
            try:
                context = {
                    name: table
                    for dep in node.dependencies
                    for name, table in contexts[dep].items()
                }
                context = {**context, **tables}
                if type(node.source) is Table and node.source.name in contexts:
                    # add the source table to the context for the node if relevant
                    context[node.source.name] = contexts[node.source.name]

                if node.type_name == "Aggregate":
                    return self.step(node, context)[node.id]
                contexts[node] = self.step(node, context)
                finished.add(node)

                for dep in node.dependents:
                    if all(d in contexts for d in dep.dependencies):
                        queue.add(dep)

                for dep in node.dependencies:
                    if all(d in finished for d in dep.dependents):
                        contexts.pop(dep)
            except Exception as e:
                raise ExecuteError(f"Step '{node.id}' failed: {e}") from e

        root = plan.root
        return contexts[root][root.id]

    @singledispatchmethod
    def step(self, node, context):
        raise NotImplementedError

    @step.register(planner.Scan)
    def scan(self, step: planner.Scan, context):
        source = step.source.name
        dataframe = context[source]

        if step.limit != math.inf:
            raise NotImplementedError("LIMIT not supported")

        if not step.projections and not step.condition:
            # ~ no op
            return {step.id: context[source]}

        return {step.id: self._project_and_filter(step, dataframe)}

    def _project_and_filter(self, step, dataframe):
        condition = self.generate(step.condition)
        projections = self.generate_tuple(step.projections)
        if condition:
            dataframe = dataframe.filter(
                lambda value: eval(condition, {**self.env, "scope": {"__self": value, None: value}})
            )
        if projections:
            print(step)
            dataframe = dataframe.apply(
                lambda value: (
                    {step.projections[i].this.this.this: eval(
                        projections[i], {**self.env, "scope": {"__self": value, None: value}}
                    ) for i in range(len(step.projections))}
                ))
        return dataframe

    def static(self):
        raise NotImplementedError

    @step.register(planner.Aggregate)
    def aggregate(self, step: planner.Aggregate, context):
        dataframe = context[step.source]
        group_by = self.generate_tuple(step.group.values())
        aggregations = self.generate_tuple(step.aggregations)
        operands = self.generate_tuple(step.operands)

        # if this is an aggregation we should always have a window to group by
        _window = [g for g in step.group.values() if g.alias_or_name in WINDOW_TYPES]
        assert len(_window) == 1
        window = _window[0]
        _interval = window.args["expressions"][1]
        interval = timedelta(**{_interval.unit.this.lower() + "s": int(_interval.this.this)})

        def initializer(value: dict) -> dict:
            return {
                ag.alias: eval(agg, {**self.env, "scope":
                    {
                        "__self": {ag.args["this"].this.name: [value[ag.args["this"].this.name]]},
                    }}) for ag, agg in zip(step.aggregations, aggregations)
            }

        def reducer(aggregated: dict, value: dict) -> dict:
            return {
                ag.alias: eval(agg, {**self.env, "scope":
                    {
                        "__self": {ag.args["this"].this.name: [aggregated[ag.alias], value[ag.args["this"].this.name]]},
                    }}) for ag, agg in zip(step.aggregations, aggregations)
            }

        dataframe = (
            getattr(dataframe, WINDOW_TYPES[window.alias_or_name])(duration_ms=interval)
            .reduce(reducer=reducer, initializer=initializer)
            .final()
        )

        for projection in step.projections:
            if type(projection) == Alias:
                to = projection.alias
                from_ = projection.this.alias_or_name.lower().split(window.alias_or_name.lower() + "_")[1]
            elif type(projection) == Column:
                continue
            else:
                raise Exception("Other projections on aggregations not supported")

        def reshape(value):
            return {
                to: datetime.fromtimestamp(value[from_] / 1000.0).isoformat(),
                **value["value"]
            }

        dataframe = dataframe.apply(reshape)

        # TODO: handle additional non-window groups. i.e. window + another column (e.g. user_id)

        return {step.id: dataframe}

    # def step(self, node: planner.Join, context):
    #     # TODO: requires API that works on multiple dataframes statefully
    #     # Also needs windows first

    # def step(self, node: planner.Sort, context):
    #     # TODO: we can do this with stateful processing but need windows first

    # def step(self, step: planner.SetOperation, context):
    #     # TODO: requires API that works on multiple dataframes statefully
    #     # Also needs windows first

    def generate(self, expression) -> str:
        """Convert a SQL expression into literal Python code and compile it into bytecode."""
        if not expression:
            return None
        return self.generator.generate(expression)

    def generate_tuple(self, expressions):
        """Convert an array of SQL expressions into tuple of Python byte code."""
        if not expressions:
            return tuple()
        return tuple(self.generate(expression) for expression in expressions)

# The default COUNT function doesn't work incrementally
def _sql_count(args):
    if len(args) == 1:
        return 1  # initialize
    return args[0] + 1
