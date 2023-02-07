# https://stackoverflow.com/a/1094423/2842217
import sys
import traceback

class EventHook(object):

    def __init__(self, on_first_sub=None, on_last_unsub=None, name:str=None):
        self._on_first_sub = on_first_sub
        self._on_last_unsub = on_last_unsub
        self._handlers = []
        self.name = name

    def __iadd__(self, handler):
        if self._on_first_sub is not None and len(self._handlers) == 0:
            self._on_first_sub()
        self._handlers.append(handler)
        return self

    def __isub__(self, handler):
        self._handlers.remove(handler)
        if self._on_last_unsub is not None and len(self._handlers) == 0:
            self._on_last_unsub()
        return self

    def fire(self, *args, **keywargs):
        for handler in self._handlers:
            try:
                handler(*args, **keywargs)
            except:
                if self.name is None:
                    print('Event handler callback failed: ')
                else:
                    print('Event handler "' + self.name + '" callback failed: ')
                traceback.print_exc()

