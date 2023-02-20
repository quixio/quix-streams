import ctypes
from ctypes import c_void_p
import os
import sysconfig


class InteropUtils(object):
    DebugEnabled = False

    lib = None
    interop_func = None

    @staticmethod
    def set_lib(lib):
        InteropUtils.lib = lib
        interoputils_enabledebug = getattr(lib, "interoputils_enabledebug")
        interoputils_disabledebug = getattr(lib, "interoputils_disabledebug")
        
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
       
    @staticmethod
    def get_function(method):
        func = getattr(InteropUtils.lib, method)
        return func

    @staticmethod
    def invoke(method, *args):
        func = InteropUtils.get_function(method)
        return func(*args)

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
        uptr = InteropUtils.allocate_uptr(len(str_bytes) + 1) # must be null terminated
        ctypes.memmove(uptr, str_bytes, len(str_bytes))
        null_terminator = ctypes.c_ubyte.from_address(uptr.value + len(str_bytes))
        null_terminator.value = 0 # allocate_uptr doesn't guarantee memory with all 0, must set manually
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

        temp_nullable = type("Nullable_struct_"+underlying_ctype.__name__, (ctypes.Structure, ), {
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

        temp_nullable = type("Nullable_mem_"+underlying_ctype.__name__, (ctypes.Structure, ), {
            '_pack_': 1,
            '_fields_': [("HasValue", ctypes.c_bool),
                        ("Value", underlying_ctype)],
            '__init__': null_init
        })

        InteropUtils.dict_mem_nullables[underlying_ctype] = temp_nullable

        return temp_nullable