import weakref


def _dummy(*args, **kwargs):
    pass


def nativedecorator(cls):
    orig_init = cls.__init__
    orig_finalizer = getattr(cls, "_finalizerfunc", _dummy)
    orig_enter = getattr(cls, "__enter__", _dummy)
    orig_exit = getattr(cls, "__exit__", _dummy)
    orig_dispose = getattr(cls, "dispose", _dummy)

    def new_init(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        self._nativedecorator_finalizer = weakref.finalize(self, new_finalizerfunc, self)

    def new_finalizerfunc(self):
        orig_finalizer(self)
        getattr(self, "_interop").dispose_ptr__()
        self._nativedecorator_finalizer.detach()

    def new_enter(self):
        orig_enter(self)

    def new_exit(self, exc_type, exc_val, exc_tb):
        orig_exit(self, exc_type, exc_val, exc_tb)
        new_dispose(self)

    def new_dispose(self, *args, **kwargs):
        if not self._nativedecorator_finalizer.alive:
            return
        orig_dispose(self, *args, **kwargs)
        self._nativedecorator_finalizer()

    cls.__init__ = new_init
    cls._finalizerfunc = new_finalizerfunc
    cls.__enter__ = new_enter
    cls.__exit__ = new_exit
    cls.dispose = new_dispose
    return cls
