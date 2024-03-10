import logging
import math
from functools import singledispatchmethod
from typing import Dict

from sqlglot import exp, planner, optimizer
from sqlglot.errors import ExecuteError
from sqlglot.executor.env import ENV
from sqlglot.executor.python import Python
from sqlglot.optimizer import build_scope

from sqlglot.planner import Plan
from sqlglot import parse_one

logger = logging.getLogger(__name__)


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
        self.env = {**ENV, **(env or {}), "scope": {}}

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
            try:
                context = {
                    name: table
                    for dep in node.dependencies
                    for name, table in contexts[dep].items()
                }
                if node.source.name in contexts:
                    # add the source table to the context for the node if relevant
                    context[node.source.name] = contexts[node.source.name]
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
        return contexts[root][root.name]

    @singledispatchmethod
    def step(self, node, context):
        raise NotImplementedError

    def step(self, step: planner.Scan, context):
        source = step.source.name
        dataframe = context[source]

        if step.limit != math.inf:
            raise NotImplementedError("LIMIT not supported")

        if not step.projections and not step.condition:
            # ~ no op
            return {step.name: context[source]}

        return {step.name: self._project_and_filter(step, dataframe)}

    def _project_and_filter(self, step, dataframe):
        condition = self.generate(step.condition)
        projections = self.generate_tuple(step.projections)
        if condition:
            dataframe = dataframe.filter(
                lambda value: eval(condition, {**self.env, "scope": {"__self": value, None: value}})
            )
        if projections:
            dataframe = dataframe.apply(
                lambda value: (
                    {step.projections[i].this.this.this: eval(
                        projections[i], {**self.env, "scope": {"__self": value, None: value}}
                    ) for i in range(len(step.projections))}
                ))
        return dataframe

    def static(self):
        raise NotImplementedError

    # def step(self, node: planner.Join, context):
    #     # TODO: requires API that works on multiple dataframes statefully
    #     # Also needs windows first

    # def step(self, node: planner.Aggregate, df):
    #     # TODO: we can do this with stateful processing but need windows first

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
