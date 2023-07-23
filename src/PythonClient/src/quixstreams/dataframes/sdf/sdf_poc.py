from functools import wraps
from copy import deepcopy


class Pipeline:
    def __init__(self, _idx=None):
        self.data = {}
        self.filtered = False
        self.executions = []
        self._idx = _idx if _idx is not None else id(self)

    @staticmethod
    def verbose_log(func):
        @wraps(func)
        def wrapped(self, *args, **kwargs):
            print(f'P-{self._idx} executing {func.__name__} with args={args}, kwargs={kwargs}')
            res = func(self, *args, **kwargs)
            print(f'P-{self._idx} data after executing {func.__name__}: {self.data}')
            return res
        return wrapped

    @staticmethod
    def _add_execution(func):
        def wrapped(self, *args, **kwargs):
            self.executions.append([func, self, args, kwargs])
        return wrapped

    @_add_execution
    @verbose_log
    def get(self, key):
        if callable(key):  # SDF outputs
            print(f'{key} is callable')
            key = key()
            if isinstance(key, bool):  # I might be able to figure out something better for this later
                return  # booleans are the remnants of SDF filtering requests handled via "filter"; this is an operational placeholder
        self.data = self.data[key]

    @_add_execution
    @verbose_log
    def add(self, *args):
        args = list(args)
        print(args)
        for i in range(len(args)):
            args[i] = args[i]() if callable(args[i]) else self.data[args[i]]
        self.data = sum(args)

    @_add_execution
    @verbose_log
    def filter(self, func):
        keep = func(self.data)
        if not isinstance(keep, bool):
            keep = all(keep)
        if keep:
            self.data = True
        else:
            self.filtered = True
            self.data = None
            print('event was filtered! Pipeline will stop')

    @_add_execution
    @verbose_log
    def select(self, keys):
        print(f'selecting keys for {self.data}: {keys}')
        if isinstance(keys, str):
            keys = [keys]
        self.data = {k: self.data[k] for k in keys}

    @_add_execution
    @verbose_log
    def set_keys(self, pairs: dict):
        print(f'setting keys for {self.data}: {pairs}')
        for k, v in pairs.items():
            if callable(v):
                v = v()
            self.data[k] = v

    @_add_execution
    def apply(self, func):
        self.data = func(self.data)

    def process(self, data):
        self.data = data
        print(f'P-{self._idx} processing with {self.data}')
        self.filtered = False
        print(f'P-{self._idx} executions are:\n{[(op[0], op[2], op[3]) for op in self.executions]}')
        for op in self.executions:
            op[0](op[1], *op[2], **op[3])
            if self.filtered:
                print(f'P-{self._idx} ops were stopped due to it being filtered; returning None')
                self.data = None
                break
        return self.data


class FilterResult:
    def __init__(self, sdf_obj=None, _result=None):
        self.sdf_obj = sdf_obj
        self._result = _result

    def output(self):
        if self._result:
            return self._result()
        return self.sdf_obj.output

    def __and__(self, other):
        print('FilterResult AND called')
        return FilterResult(_result=lambda: (self.output() and other.output()))


class OperationResult:
    def __init__(self, sdf_obj=None):
        self.sdf_obj = sdf_obj

    def _get_output(self):
        return self.sdf_obj.output


class SDF:
    instances = {}
    outputs = {}
    latest_absolute_state_idx = 0
    _instance_idx = -1

    @classmethod
    def _set_abs_state(cls, state):
        print(f'latest_state updated from {cls.latest_absolute_state_idx} to {state}')
        cls.latest_absolute_state_idx = state

    def __new__(cls, *args, **kwargs):
        cls._instance_idx += 1
        new = super().__new__(cls)
        cls.instances[cls._instance_idx] = new
        return new

    def __init__(self, _pipeline=None):
        self.input = None
        self.output = None
        self._idx = self._instance_idx
        self.pipeline = _pipeline if _pipeline else Pipeline(_idx=self._idx)
        self._dependency_idx = SDF.latest_absolute_state_idx
        print(f'SDF-{self._idx} with dependency SDF-{self._dependency_idx} created')

    def _get_output(self):
        return self.output

    @staticmethod
    def override_with_latest_state(func):
        @wraps(func)
        def wrapped(self, *args, **kwargs):
            if self.latest_absolute_state_idx and (self._idx < self.latest_absolute_state_idx):
                print(f'overriding object instance {self._idx} with the latest, {self.latest_absolute_state_idx}')
                self = SDF.instances[SDF.latest_absolute_state_idx]
            return func(self, *args, **kwargs)
        return wrapped

    @staticmethod
    def verbose_log(func):
        @wraps(func)
        def wrapped(self, *args, **kwargs):
            print_args = []
            for arg in args:
                print_args.append(f'SDF-{arg._idx} OUTPUT_RESULT' if isinstance(arg, SDF) else arg)
            print(f'SDF-{self._idx} calling {func.__name__} with args={print_args}, kwargs={kwargs}')
            return func(self, *args, **kwargs)
        return wrapped

    @verbose_log
    @override_with_latest_state
    def __getitem__(self, item):
        print('SDF item retrieval causes a new SDF to be made, and desired retrieval added to it instead')
        new_sdf = SDF()
        if isinstance(item, str):
            new_sdf.pipeline.get(item)
        else:
            SDF._set_abs_state(new_sdf._idx)
            if isinstance(item, FilterResult):
                new_sdf.pipeline.get(item.output)
            else:
                new_sdf.pipeline.select(item)
        return new_sdf

    @verbose_log
    @override_with_latest_state
    def __add__(self, other):
        new_sdf = SDF()
        if isinstance(other, SDF):
            other = other._get_output
        new_sdf.pipeline.add(self._get_output, other)
        return OperationResult(new_sdf)

    @verbose_log
    @override_with_latest_state
    def __ge__(self, other):
        def ge_filter(data):
            return data >= other
        self.pipeline.filter(ge_filter)
        return FilterResult(sdf_obj=self)

    @verbose_log
    @override_with_latest_state
    def __setitem__(self, key, value):
        if isinstance(value, (SDF, OperationResult)):
            new_sdf = SDF()
            self._set_abs_state(new_sdf._idx)
            value = value._get_output
        else:
            new_sdf = self
        new_sdf.pipeline.set_keys({key: value})
        return new_sdf

    @verbose_log
    @override_with_latest_state
    def apply(self, func):
        self.pipeline.apply(func)
        return self

    def _process(self):
        if self._dependency_idx == self._idx:
            pipeline_input = self.input
            print(f'no dependency for {self._idx}')
        else:
            if self._dependency_idx not in SDF.outputs:
                print(f'dependency output for SDF-{self._dependency_idx} is missing...will process it!')
                SDF.instances[self._dependency_idx]._process()
            pipeline_input = self.outputs[self._dependency_idx]
            print(f'dependency SDF-{self._dependency_idx} output for SDF-{self._idx} is {pipeline_input}; using as input for SDF-{self._idx}')
        self.output = self.pipeline.process(deepcopy(pipeline_input))

    @staticmethod
    def process(data):
        SDF.outputs = {}
        SDF.instances[0].input = data
        print(f'\n\nSDF processing instances {SDF.instances}')
        print(f'final result will come from SDF instance {SDF.latest_absolute_state_idx}')
        for k, v in SDF.instances.items():
            print(f'processing SDF instance {k}')
            v._process()
            SDF.outputs[k] = v.output
            if v.pipeline.filtered:
                print('results were filtered, returning None')
                return None
        return SDF.outputs[SDF.latest_absolute_state_idx]


def plus_10(x):
    return x + 10


def plus_50(x):
    return x + 50


def add_20_to_all(x):
    return {k: v + 20 for k, v in x.items()}


sdf = SDF()
sdf = sdf[['a', 'b']]
sdf['d'] = sdf['b'].apply(plus_10)
sdf['e'] = sdf['a'] + sdf['b']
sdf = sdf[(sdf['d'] >= 10) & (sdf['a'] >= 2)].apply(add_20_to_all)
sdf['f'] = sdf['d'].apply(plus_50)
sdf_res1 = sdf.process({'a': 5, 'b': 6, 'c': 7})
sdf_res2 = sdf.process({'a': 1, 'b': 11, 'c': 12})
sdf_res3 = sdf.process({'a': 10, 'b': 11, 'c': 12})
print(f'result: {sdf_res1}')
print(f'result: {sdf_res2} (should be None due to filtering)')
print(f'result: {sdf_res3}')