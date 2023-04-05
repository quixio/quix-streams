import ctypes
import threading
from ctypes import c_void_p
import os
import sysconfig
from typing import Callable


class InteropException(Exception):

    def __init__(self, exc_type: str, message: str, exc_stack: str):
        self.exc_type = exc_type
        self.message = message
        self.exc_stack = exc_stack
        super().__init__(self.message + "\n" + exc_stack)


class InteropUtils(object):
    DebugEnabled = False

    lib = None
    interop_func = None

    __last_exception = None
    __interop_exception_handler_ref = None
    __exception_handler = None

    @staticmethod
    def set_lib(lib, with_debug_enabled=False):
        InteropUtils.lib = lib

        interoputils_enabledebug = getattr(lib, "interoputils_enabledebug")
        interoputils_disabledebug = getattr(lib, "interoputils_disabledebug")
        if with_debug_enabled:
            InteropUtils.enable_debug()

        interoputils_log_debug = getattr(lib, "interoputils_log_debug")
        interoputils_log_debug.argtypes = [c_void_p]

        interoputils_alloc_uptr = getattr(lib, "interoputils_alloc_uptr")
        interoputils_alloc_uptr.argtypes = [ctypes.c_int32]
        interoputils_alloc_uptr.restype = c_void_p

        interoputils_pin_hptr_target = getattr(lib, "interoputils_pin_hptr_target")
        interoputils_pin_hptr_target.argtypes = [c_void_p]
        interoputils_pin_hptr_target.restype = c_void_p

        interoputils_get_pin_address = getattr(lib, "interoputils_get_pin_address")
        interoputils_get_pin_address.argtypes = [c_void_p]
        interoputils_get_pin_address.restype = c_void_p

        interoputils_free_hptr = getattr(lib, "interoputils_free_hptr")
        interoputils_free_hptr.argtypes = [c_void_p]

        interoputils_free_uptr = getattr(lib, "interoputils_free_uptr")
        interoputils_free_uptr.argtypes = [c_void_p]

        interoputils_set_exception_callback = getattr(lib, "interoputils_set_exception_callback")
        interoputils_set_exception_callback.argtypes = [c_void_p]

        def set_exception_to_raise(interop_ex: InteropException):
            InteropUtils.__last_exception = interop_ex

        InteropUtils.__set_exception_callback(set_exception_to_raise)

        def raise_interop_exception(interop_ex: InteropException):
            raise interop_ex

        InteropUtils.set_exception_callback(raise_interop_exception)


    @staticmethod
    def get_function(method):
        func = getattr(InteropUtils.lib, method)
        return func

    @staticmethod
    def invoke(method, *args):
        func = InteropUtils.get_function(method)
        res = func(*args)
        if InteropUtils.__last_exception is None:
            return res

        ex = InteropUtils.__last_exception
        InteropUtils.__last_exception = None
        InteropUtils.__exception_handler(ex)


    @staticmethod
    def ptr_to_utf8(pointer: ctypes.c_char_p) -> str:
        if pointer is None:
            return None
        val = ctypes.cast(pointer, ctypes.c_char_p).value.decode('utf-8')
        return val

    @staticmethod
    def utf8_to_ptr(string) -> ctypes.c_char_p:
        if string is None:
            return None
        # TODO this can likely be done eaiser with c_char_p or sth, but it has to be freeable using Marshal.FreeHGlobal
        str_bytes = string.encode('utf-8')
        uptr = InteropUtils.allocate_uptr(len(str_bytes) + 1)  # must be null terminated
        ctypes.memmove(uptr, str_bytes, len(str_bytes))
        null_terminator = ctypes.c_ubyte.from_address(uptr.value + len(str_bytes))
        null_terminator.value = 0  # allocate_uptr doesn't guarantee memory with all 0, must set manually
        return uptr

    @staticmethod
    def uptr_to_utf8(pointer: c_void_p) -> str:
        if pointer is None:
            return None
        val = ctypes.c_char_p(pointer.value).value.decode('utf-8')
        InteropUtils.free_uptr(pointer)
        return val

    @staticmethod
    def utf8_to_uptr(string: str) -> c_void_p:
        if string is None:
            return c_void_p(None)

        bytes = string.encode('utf-8')  # will get GC'd
        bytes_len = len(bytes) + 1  # +1 because ultimately we want a null terminated array of bytes
        uptr = InteropUtils.allocate_uptr(bytes_len)
        ctypes.memmove(uptr, bytes, bytes_len)
        return uptr

    @staticmethod
    def set_exception_callback(callback: Callable[[InteropException], None]):
        """
        Sets the exception handler for interop exceptions

        callback: Callable[[InteropException], None]
            The callback which takes InteropException and returns nothing
        """
        InteropUtils.__exception_handler = callback

    @staticmethod
    def __set_exception_callback(callback: Callable[[InteropException], None]):
        """
        Sets the exception handler for the library
        
        callback: Callable[[InteropException], None]
            The callback which takes InteropException and returns nothing
        """

        def converter(exc_type_ptr, exc_message_ptr, exc_stack_ptr):
            exc_type = InteropUtils.ptr_to_utf8(exc_type_ptr)
            exc_message = InteropUtils.ptr_to_utf8(exc_message_ptr)
            exc_stack = InteropUtils.ptr_to_utf8(exc_stack_ptr)
            callback(InteropException(exc_type, exc_message, exc_stack))

        wrapper = ctypes.CFUNCTYPE(None, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p)(converter)
        wrapper_addr = ctypes.cast(wrapper, c_void_p)

        InteropUtils.invoke("interoputils_set_exception_callback", wrapper_addr)
        InteropUtils.__interop_exception_handler_ref = wrapper_addr  # avoid GC'ing it

    @staticmethod
    def log_debug(message: str):
        """
        Logs debug message if debugging is enabled
        
        message: str
            The message to log
        """

        if not InteropUtils.DebugEnabled:
            return

        message_ptr = InteropUtils.utf8_to_ptr(message)

        InteropUtils.invoke("interoputils_log_debug", message_ptr)

    @staticmethod
    def enable_debug():
        """
        Enables Debugging logs
        """

        InteropUtils.DebugEnabled = True
        InteropUtils.invoke("interoputils_enabledebug")

    @staticmethod
    def disable_debug():
        """
        Enables Debugging logs
        """

        InteropUtils.DebugEnabled = False
        InteropUtils.invoke("interoputils_disabledebug")

    @staticmethod
    def pin_hptr_target(hptr: c_void_p) -> c_void_p:
        """
        Creates a GC Handle with pinned type.
        Use get_pin_address to acquire the pinned resources's address
        Must be freed as soon as possible with free_hptr

        Parameters
        ----------

        hptr: c_void_p
            Pointer to .Net GC Handle

        Returns
        -------
        c_void_p:
            .Net pointer to the value
        """

        return InteropUtils.invoke("interoputils_pin_hptr_target", hptr)

    @staticmethod
    def get_pin_address(hptr: c_void_p) -> c_void_p:
        """
        Retrieves the address of the pinned resource from the provided GC Handle ptr

        Parameters
        ----------

        hptr: c_void_p
            Pointer to .Net pinned GC Handle

        Returns
        -------
        c_void_p:
            memory address of the underlying resource
        """

        return InteropUtils.invoke("interoputils_get_pin_address", hptr)

    @staticmethod
    def free_hptr(hptr: c_void_p) -> None:
        """
        Frees the provided GC Handle

        Parameters
        ----------

        hptr: c_void_p
            Pointer to .Net GC Handle
        """

        return InteropUtils.invoke("interoputils_free_hptr", hptr)

    @staticmethod
    def free_uptr(uptr: c_void_p) -> None:
        """
        Frees the provided unmanaged pointer

        Parameters
        ----------

        uptr: c_void_p
            Unmanaged pointer
        """

        return InteropUtils.invoke("interoputils_free_uptr", uptr)

    @staticmethod
    def allocate_uptr(size: int) -> c_void_p:
        """
        Allocated unmanaged memory pointer of the desired size

        Parameters
        ----------

        size: c_void_p
            The desired size of the memory

        Returns
        -------
        c_void_p:
            Unmanaged memory pointer
        """

        return c_void_p(InteropUtils.invoke("interoputils_alloc_uptr", size))

    @staticmethod
    def invoke_and_free(hptr: ctypes.c_void_p, func, *args, **kwargs):
        """
        Invokes a function where first positional argument is a c# GC Handle pointer (hptr), then disposes it

        Parameters
        ----------

        hptr: ctypes.c_void_p
            c# GC Handle pointer (hptr)

        func:
            callable function where first arg is the hptr

        Returns
        -------
        Any:
            the function result            
        """
        if hptr is None:
            return kwargs["default"]

        try:
            return func(hptr, *args)
        finally:
            InteropUtils.free_hptr(hptr)

    dict_nullables = {}  # TODO Should I lock on this?

    @staticmethod
    def create_nullable(underlying_ctype):
        """
        Create a nullable type that is equivalent for serialization purposes to a nullable c# type in places where it is 
        serialized as a struct. In this case byte prefix is added equivalent to minimum addressable memory size according
        to cpu architecture. For example a nullable double on 64 bit would be 8 bytes for boolean and 8 bytes for double.

        Parameters
        ----------

        underlying_ctype:
            type defined by ctypes such as ctypes.c_double
        """

        if underlying_ctype in InteropUtils.dict_nullables:
            return InteropUtils.dict_nullables[underlying_ctype]

        def null_init(self, value):
            self.HasValue = value is not None
            if self.HasValue:
                self.Value = value

        # This is the nullable type to use when C# Marshals it as a struct (rather than a pointer). In this case the flag is stored
        # on the minimum addressable memory size (8 bytes on 64 bit systems, 4 on 32). ctypes should pack it accordingly

        temp_nullable = type("Nullable_struct_" + underlying_ctype.__name__, (ctypes.Structure,), {
            '_fields_': [("HasValue", ctypes.c_bool),
                         ("Value", underlying_ctype)],
            '__init__': null_init
        })

        InteropUtils.dict_nullables[underlying_ctype] = temp_nullable

        return temp_nullable

    dict_mem_nullables = {}  # TODO Should I lock on this?

    @staticmethod
    def create_mem_nullable(underlying_ctype):
        """
        Create a nullable type that is equivalent for serialization purposes to a nullable c# type in places where it is not 
        serialized as a struct, but rather as a continuous memory segment to pointed at by either a pointer or array.
        In this case 1 byte prefix is added kept for the boolean flag rather than the min addressable memory size according
        to cpu architecture. For example a nullable double on 64 bit would be 1 byte for boolean and 8 bytes for double.

        Parameters
        ----------

        underlying_ctype:
            type defined by ctypes such as ctypes.c_double
        """

        if underlying_ctype in InteropUtils.dict_mem_nullables:
            return InteropUtils.dict_mem_nullables[underlying_ctype]

        def null_init(self, value):
            self.HasValue = value is not None
            if self.HasValue:
                self.Value = value

        temp_nullable = type("Nullable_mem_" + underlying_ctype.__name__, (ctypes.Structure,), {
            '_pack_': 1,
            '_fields_': [("HasValue", ctypes.c_bool),
                         ("Value", underlying_ctype)],
            '__init__': null_init
        })

        InteropUtils.dict_mem_nullables[underlying_ctype] = temp_nullable

        return temp_nullable
