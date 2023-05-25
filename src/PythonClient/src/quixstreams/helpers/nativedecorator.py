import weakref


def _dummy(*args, **kwargs):
    pass


def nativedecorator(cls):
    orig_init = cls.__init__
    orig_finalizer = getattr(cls, "_finalizerfunc", _dummy)
    orig_enter = getattr(cls, "__enter__", _dummy)
    orig_exit = getattr(cls, "__exit__", _dummy)
    orig_dispose = getattr(cls, "dispose", _dummy)
    orig_del = getattr(cls, "__del__", _dummy)

    def new_init(self, *args, **kwargs):
        self._nativedecorator_finalized = False
        orig_init(self, *args, **kwargs)

    def new_finalizerfunc(self):
        if self._nativedecorator_finalized:
            return

        self._nativedecorator_finalized = True
        orig_finalizer(self)
        getattr(self, "_interop").dispose_ptr__()

    def new_del(self):
        new_finalizerfunc(self)
        orig_del(self)

    def new_enter(self):
        orig_enter(self)

    def new_exit(self, exc_type, exc_val, exc_tb):
        orig_exit(self, exc_type, exc_val, exc_tb)
        new_dispose(self)

    def new_dispose(self, *args, **kwargs):
        if self._nativedecorator_finalized:
            return

        orig_dispose(self, *args, **kwargs)

    cls.__init__ = new_init
    cls._finalizerfunc = new_finalizerfunc
    cls.__enter__ = new_enter
    cls.__exit__ = new_exit
    cls.__del__ = new_del
    cls.dispose = new_dispose
    return cls
