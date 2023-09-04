from ..native.Python.InteropHelpers.InteropUtils import InteropUtils


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
        self._nativedecorator_disposed = False
        orig_init(self, *args, **kwargs)

    def new_finalizerfunc(self):
        """
        Finalizes the underlying object
        Finalize is implementation specific but generally de-references the underlying objects which
        may or may not result in garbage collection of the de-referenced objects
        """
        if self._nativedecorator_finalized:
            return

        ptr = getattr(self, "_interop").get_interop_ptr__()

        InteropUtils.log_debug(f"Finalizing {cls.__name__} ({ptr.value}) of object {id(self)}")
        InteropUtils.log_debug_indent_increment()

        self._nativedecorator_finalized = True
        orig_finalizer(self)

        getattr(self, "_interop").dispose_ptr__()

        InteropUtils.log_debug_indent_decrement()
        InteropUtils.log_debug(f"Finalized {cls.__name__} ({ptr.value}) of object {id(self)}")


    def new_del(self):
        new_finalizerfunc(self)
        orig_del(self)

    def new_enter(self):
        return orig_enter(self)

    def new_exit(self, exc_type, exc_val, exc_tb):
        result = orig_exit(self, exc_type, exc_val, exc_tb)
        new_dispose(self)
        return result

    def new_dispose(self, *args, **kwargs) -> None:
        """
        Disposes the underlying object in addition to finalizing it
        Dispose is implementation specific but it generally frees underlying resources rather than just
        de-references
        """

        if self._nativedecorator_disposed:
            new_finalizerfunc(self)
            return

        ptr = getattr(self, "_interop").get_interop_ptr__()

        InteropUtils.log_debug(f"Disposing {cls.__name__} ({ptr.value}) of object {id(self)}")
        InteropUtils.log_debug_indent_increment()

        self._nativedecorator_disposed = True
        orig_dispose(self, *args, **kwargs)
        new_finalizerfunc(self)

        InteropUtils.log_debug_indent_decrement()
        InteropUtils.log_debug(f"Disposed {cls.__name__} ({ptr.value}) of object {id(self)}")

    cls.__init__ = new_init
    cls._finalizerfunc = new_finalizerfunc
    cls.__enter__ = new_enter
    cls.__exit__ = new_exit
    cls.__del__ = new_del
    cls.dispose = new_dispose
    return cls
