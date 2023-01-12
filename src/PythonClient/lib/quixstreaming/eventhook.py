# https://stackoverflow.com/a/1094423/2842217
import sys
import traceback

class EventHook(object):

    def __init__(self, on_first_sub=None, on_last_unsub=None, name:str=None):
        self.__on_first_sub = on_first_sub
        self.__on_last_unsub = on_last_unsub
        self.__handlers = []
        self.name = name

    def __iadd__(self, handler):
        if self.__on_first_sub is not None and len(self.__handlers) == 0:
            self.__on_first_sub()
        self.__handlers.append(handler)
        return self

    def __isub__(self, handler):
        self.__handlers.remove(handler)
        if self.__on_last_unsub is not None and len(self.__handlers) == 0:
            self.__on_last_unsub()
        return self

    def fire(self, *args, **keywargs):
        for handler in self.__handlers:
            try:
                handler(*args, **keywargs)
            except:
                if self.name is None:
                    print('Event handler callback failed: ')
                else:
                    print('Event handler "' + self.name + '" callback failed: ')
                traceback.print_exc()

