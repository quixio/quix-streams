import uuid
from typing import Self, Optional, Any, Callable, TypeAlias
import operator

OpValue: TypeAlias = int | float | bool
MessageApplier: TypeAlias = Callable[[dict], Optional[dict]]
MessageFieldApplier: TypeAlias = Callable[[dict], OpValue]


class PipelineField:
    def __init__(self, name: Optional[str] = None, _eval_func: Optional[MessageFieldApplier] = None):
        self.name = name
        self._eval_func = _eval_func if _eval_func else lambda message: message[self.name]

    @staticmethod
    def _as_pipeline_field(value):
        if not isinstance(value, PipelineField):
            return PipelineField(_eval_func=lambda message: value)
        return value

    def _operation(self, other: Any, op: Callable[OpValue, OpValue]) -> Self:
        return PipelineField(
            _eval_func=lambda message: op(self.eval(message), self._as_pipeline_field(other).eval(message)))

    def eval(self, message: dict) -> OpValue:
        return self._eval_func(message)

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
    def __init__(self, func: MessageApplier):
        self._id = str(uuid.uuid4())
        self._func = func

    def __call__(self, message: dict):
        return self._func(message)

    @property
    def id(self) -> str:
        return self._id


class Pipeline:

    def __init__(self, functions: list[PipelineFunction] = None):
        self._functions = functions or []
        self._id = str(uuid.uuid4())

    @staticmethod
    def _filter(pf: PipelineField) -> MessageApplier:
        return lambda message: message if pf.eval(message) else None

    @staticmethod
    def _subset(keys: list) -> MessageApplier:
        return lambda message: {k: message[k] for k in keys}

    @staticmethod
    def _set_item(k: str, v: OpValue, message: dict) -> dict:
        message[k] = v
        return message

    @property
    def functions(self) -> list[PipelineFunction]:
        return self._functions

    def apply(self, func: MessageApplier):
        self._functions.append(PipelineFunction(func=func))

    def process(self, message: dict) -> Optional[dict]:
        result = message
        for func in self._functions:
            result = func(result)
            if result is None:
                print('result was filtered')
                break
        return result

    def __getitem__(self, item: str | list | PipelineField) -> PipelineField | Self:
        if isinstance(item, PipelineField):
            self.apply(self._filter(item))
            return self
        elif isinstance(item, list):
            self.apply(self._subset(item))
            return self
        return PipelineField(name=item)

    def __setitem__(self, key: str, value: PipelineField | OpValue):
        if isinstance(value, PipelineField):
            self.apply(lambda message: self._set_item(key, value.eval(message), message))
        else:
            self.apply(lambda message: self._set_item(key, value, message))


if __name__ == "__main__":
    p = Pipeline()
    p = p[['x', 'y']]
    p["z"] = p["x"]
    p["z"] = p["x"] + p["y"]
    p["a"] = p["x"] + 2
    p["b"] = 12
    p = p[(p['x'] >= 1) & (p['y'] < 10)]
    result = p.process(message={"x": 1, "y": 2, "q": 3})
    print(result)
