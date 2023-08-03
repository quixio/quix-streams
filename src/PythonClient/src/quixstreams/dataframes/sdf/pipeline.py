import uuid
from typing import Self, Optional, Any, Callable, TypeAlias
import operator

OpValue: TypeAlias = int | float | bool
ColumnValue: TypeAlias = int | float | bool | list | dict
EventApplier: TypeAlias = Callable[[dict], Optional[dict]]
EventColumnApplier: TypeAlias = Callable[[dict], OpValue]


class EventColumn:
    def __init__(self, name: Optional[str] = None, _eval_func: Optional[EventColumnApplier] = None):
        self.name = name
        self._eval_func = _eval_func if _eval_func else lambda message: message[self.name]

    @staticmethod
    def _as_pipeline_field(value):
        if not isinstance(value, EventColumn):
            return EventColumn(_eval_func=lambda message: value)
        return value

    def _operation(self, other: Any, op: Callable[[OpValue, OpValue], OpValue]) -> Self:
        return EventColumn(
            _eval_func=lambda message: op(self.eval(message), self._as_pipeline_field(other).eval(message)))

    def eval(self, message: dict) -> ColumnValue:
        return self._eval_func(message)

    def apply(self, f: EventColumnApplier) -> Self:
        return EventColumn(_eval_func=lambda message: f(self.eval(message)))

    def __mod__(self, other):
        return self._operation(other, operator.mod)

    def __and__(self, other):
        return self._operation(other, operator.and_)

    def __or__(self, other):
        return self._operation(other, operator.or_)

    def __add__(self, other):
        return self._operation(other, operator.add)

    def __sub__(self, other):
        return self._operation(other, operator.sub)

    def __mul__(self, other):
        return self._operation(other, operator.mul)

    def __truediv__(self, other):
        return self._operation(other, operator.truediv)

    def __eq__(self, other):
        return self._operation(other, operator.eq)

    def __ne__(self, other):
        return self._operation(other, operator.ne)

    def __lt__(self, other):
        return self._operation(other, operator.lt)

    def __le__(self, other):
        return self._operation(other, operator.le)

    def __gt__(self, other):
        return self._operation(other, operator.gt)

    def __ge__(self, other):
        return self._operation(other, operator.ge)


class PipelineFunction:
    def __init__(self, func: EventApplier):
        self._id = str(uuid.uuid4())
        self._func = func

    def __call__(self, message: dict):
        return self._func(message)

    @property
    def id(self) -> str:
        return self._id


class MessagePipeline:

    def __init__(self, functions: list[PipelineFunction] = None):
        self._functions = functions or []
        self._id = str(uuid.uuid4())

    @staticmethod
    def _filter(pf: EventColumn) -> EventApplier:
        return lambda message: message if pf.eval(message) else None

    @staticmethod
    def _subset(keys: list) -> EventApplier:
        return lambda message: {k: message[k] for k in keys}

    @staticmethod
    def _set_item(k: str, v: OpValue, message: dict) -> dict:
        message[k] = v
        return message

    @property
    def functions(self) -> list[PipelineFunction]:
        return self._functions

    def apply(self, func: EventApplier):
        self._functions.append(PipelineFunction(func=func))

    def process(self, event: dict) -> Optional[dict]:
        result = event
        for func in self._functions:
            print(f'processing func {func._func.__name__}')
            result = func(result)
            print(result)
            if result is None:
                print('result was filtered')
                break
        return result

    def __getitem__(self, item: str | list | EventColumn) -> EventColumn | Self:
        if isinstance(item, EventColumn):
            self.apply(self._filter(item))
            return self
        elif isinstance(item, list):
            self.apply(self._subset(item))
            return self
        return EventColumn(name=item)

    def __setitem__(self, key: str, value: EventColumn | OpValue):
        if isinstance(value, EventColumn):
            self.apply(lambda message: self._set_item(key, value.eval(message), message))
        else:
            self.apply(lambda message: self._set_item(key, value, message))


if __name__ == "__main__":
    p = MessagePipeline()
    p = p[['x', 'y']]
    p["z"] = p["x"].apply(lambda _: _+500)
    p["a"] = p["x"] + p["y"]
    p["b"] = 12
    p = p[(p['x'] >= 1) & (p['y'] < 10)]
    result = p.process(event={"x": 1, "y": 2, "q": 3})
    print(result)
