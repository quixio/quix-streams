using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

// leave it as InteropHelpers.Interop.
namespace InteropHelpers.Interop;

public class InteropUtils
{
    /// <summary>
    /// When nullable is represented in memory as a continuous memory segment either in array or at a pointer, 1 byte prefix is added kept for the boolean flag
    /// This is not the same as when C# Marshals it as a struct (rather than a pointer), in that case the flag is stored
    /// on the minimum addressable memory size (8 bytes on 64 bit systems, 4 on 32)
    /// </summary>
    private static int NullableHasValuePrefixSize = 1;
    
    public static bool DebugMode = false;
    private static Lazy<StreamWriter> debuglogs = new Lazy<StreamWriter>(() => File.AppendText($"./debuglogs_{(DateTime.Now.ToString("yyyy-MM-dd_HH_mm_ss"))}.txt"));
    private static object debugLogsLock = new object();
    private static int debugLogIndent = 0;
    private const int indentSize = 2;


    public static void LogDebug(string format, params object[] @params)
    {
        if (!DebugMode) return;
        // Due to importance of having every line available for debugging, the performance impact is acceptable here
        // in order to not have potential for incorrect log write
        lock (debugLogsLock)
        {
            int threadId = Thread.CurrentThread.ManagedThreadId;
            debuglogs.Value.Write($"[{DateTime.Now:yy-MM-dd HH:mm:ss.fff}][{threadId}]  ");
            if (debugLogIndent > 0) debuglogs.Value.Write(new string(' ', debugLogIndent));
            debuglogs.Value.WriteLine(string.Format(format, @params));

            debuglogs.Value.Flush();
        }
    }
    
    public static void LogDebugIndentIncr()
    {
        if (!DebugMode) return;
        lock (debugLogsLock)
        {
            debugLogIndent += indentSize;
        }
    }
    public static void LogDebugIndentDecr()
    {
        if (!DebugMode) return;
        lock (debugLogsLock)
        {
            debugLogIndent = Math.Max(0, debugLogIndent-indentSize);
        }
    }
    
    /// <summary>
    /// Creates a handler for the object and returns the pointer to it
    /// </summary>
    public static IntPtr ToHPtr<T>(T obj)
    {
        if (obj == null) return IntPtr.Zero;
        var handle = GCHandle.Alloc(obj);
        var ptr = GCHandle.ToIntPtr(handle);
        LogDebug("Allocated Ptr: {0}, type: {1}, {2}", ptr,typeof(T).FullName, obj == null ? "is null" : "is not null");
        return ptr;
    }
    
    #region UPtr
    /// <summary>
    /// Allocates unmanaged memory for the object and returns pointer to it
    /// </summary>
    public static IntPtr ToUPtr<T>(T obj)
    {
        return ToUPtr(obj, typeof(T));
    }
    
    /// <summary>
    /// Allocates unmanaged memory for the object and returns pointer to it
    /// </summary>
    public static IntPtr ToUPtr(object obj, Type objectType)
    {
        if (obj == null) return IntPtr.Zero;

        IntPtr ptr;
        if (obj is Array array)
        {
            var elementType = objectType.GetElementType();
            return ToArrayUPtr(array, elementType);
        }
        
        LogDebug("Checking underlying nullable type for " + objectType);
        var underlyingNullable = Nullable.GetUnderlyingType(objectType);
        if (underlyingNullable != null)
        {
            return ToNullableUPtr(obj, underlyingNullable);
        }
        var size = Marshal.SizeOf(obj);
        ptr = Marshal.AllocHGlobal(size);
        // Copy the struct to the memory block
        Marshal.StructureToPtr(obj, ptr, false);
        
        LogDebug("Allocated UPtr: {0}, type: {1}, {2}", ptr, objectType.FullName, "is not null");
        return ptr;
    }

    public static IntPtr ToArrayUPtr(Array array, Type elementType)
    {
        if (elementType == null)
        {
            LogDebug(new System.Diagnostics.StackTrace().ToString());
            throw new Exception("Type not set");
        }

        if (array == null)
        {
            return IntPtr.Zero;
        }

        if (!elementType.IsValueType)
        {
            LogDebug($"Converting non-value array to pointer array for type: {elementType}");

            var pointerArray = new IntPtr[array.Length];
            var arrayIndex = 0;
            foreach (var element in array)
            {
                pointerArray[arrayIndex] = ToHPtr(element);
                arrayIndex++;
            }

            elementType = typeof(IntPtr);
            array = pointerArray;
        }
        LogDebug($"Array UPtr attempt for type: {elementType}");

        var underlyingNullable = Nullable.GetUnderlyingType(elementType);
        if (underlyingNullable != null)
        {
            return ToNullableArrayUPtr(array, underlyingNullable);
        }


        // not checking for blittable, let the framework deal with it
        const int countSize = 4;
        var elementSize = Marshal.SizeOf(elementType);
        var size = elementSize * array.Length; 
        size += countSize; // for the item count as integer
        var ptr = Marshal.AllocHGlobal(size);
        
        LogDebug($"Array length: {array.Length} at ptr {ptr}, array starting at {ptr+countSize}, with element size {elementSize} | 46a6f");
        Marshal.StructureToPtr(array.Length, ptr, false);
        
        // Marshalling element by element because in native AOT we're limited in what we can do
        // and System.Byte[] refused to marshal for me. Maybe there is a fix?
        var index = 0;
        foreach (var val in array)
        {
            Marshal.StructureToPtr(val, ptr+countSize+elementSize*index, false);
            index++;
        }
        LogDebug("Allocated UPtr: {0}, type: {1}, {2}", ptr, elementType.FullName+"[]", "is not null");
        return ptr;
    }
    
    private static IntPtr ToNullableUPtr(object obj, Type underlyingType)
    {
        var elementSize = NullableHasValuePrefixSize;
        
        // not checking for blittable, let the framework deal with it
        var underlyingElementSize = Marshal.SizeOf(underlyingType);
        elementSize += underlyingElementSize;
        var ptr = Marshal.AllocHGlobal(elementSize);

        var currentPtr = ptr;
        var hasValue = obj != null; 
        Marshal.StructureToPtr(hasValue, currentPtr, false);
        if (hasValue)
        {
            var actual = Convert.ChangeType(obj, underlyingType);
            Marshal.StructureToPtr(actual, currentPtr, false);
        }

        return ptr;
    }
    
    private static IntPtr ToNullableArrayUPtr(Array array, Type underlyingType)
    {
        var elementSize = NullableHasValuePrefixSize;
        
        // not checking for blittable, let the framework deal with it
        const int countSize = 4;
        var underlyingElementSize = Marshal.SizeOf(underlyingType);
        elementSize += underlyingElementSize;
        var size = elementSize * array.Length; 
        size += countSize; // for the item count as integer
        var ptrStart = Marshal.AllocHGlobal(size);

        var currentPtr = ptrStart;
        LogDebug($"Array length: {array.Length} at ptr {ptrStart}, array starting at {ptrStart+countSize}, with element size {elementSize} for underlying type {underlyingType} | 5ac15");
        Marshal.StructureToPtr(array.Length, currentPtr, false);
        currentPtr += countSize;

        // Marshalling element by element because in native AOT we're limited in what we can do
        var index = 0;
        foreach (var val in array)
        {
            var hasValue = val != null; 
            Marshal.StructureToPtr(hasValue, currentPtr, false);
            currentPtr += NullableHasValuePrefixSize;
            if (hasValue)
            {
                var actual = Convert.ChangeType(val, underlyingType);
                Marshal.StructureToPtr(actual, currentPtr, false);
            }
            currentPtr += underlyingElementSize;

            index++;
        }

        if (DebugMode)
        {
            LogDebug("Allocated UPtr: {0}, type: {1}, {2}", ptrStart, underlyingType.FullName + "[]", "is not null");
            var bytes = new byte[(long)currentPtr - (long)ptrStart];
            Marshal.Copy(ptrStart, bytes, 0, bytes.Length);
            LogDebug($"Bytes: {BitConverter.ToString(bytes).Replace("-","")} | 5ac15");
        }
        
        return ptrStart;
    }
    
    public static T FromUPtr<T>(IntPtr uptr)
    {
        return (T)FromUPtr(uptr, typeof(T));
    }
    
    public static object FromUPtr(IntPtr uptr, Type objectType)
    {
        if (uptr == IntPtr.Zero) return null;

        object obj;
        if (typeof(Array).IsAssignableFrom(objectType))
        {
            var elementType = objectType.GetElementType();
            return FromArrayUPtr(uptr, elementType);
        }
        else
        {
            LogDebug("Checking underlying nullable type for " + objectType);
            var underlyingNullable = Nullable.GetUnderlyingType(objectType);
            if (underlyingNullable != null)
            {
                return FromNullableUPtr(uptr, underlyingNullable);
            }
            obj = Marshal.PtrToStructure(uptr, objectType);
        }
        
        LogDebug("Converted UPtr: {0}, type: {1}, {2}", uptr, objectType.FullName, "is not null");
        FreeUPtr(uptr);
        return obj;
    }

    public static Array FromArrayUPtr(IntPtr uptr, Type elementType)
    {
        if (uptr == IntPtr.Zero) return null;
        if (elementType == null)
        {
            LogDebug(new System.Diagnostics.StackTrace().ToString());
        }
        LogDebug($"Array UPtr conversion attempt for type: {elementType}");
        var underlyingNullable = Nullable.GetUnderlyingType(elementType);
        if (underlyingNullable != null)
        {
            return FromNullableArrayUPtr(uptr, underlyingNullable);
        }
        // not checking for blittable, let the framework deal with it
        const int countSize = 4;
        var elementSize = Marshal.SizeOf(elementType);
        var length = Marshal.PtrToStructure<int>(uptr);
        var currentPtr = uptr + countSize;
        var ptrEnd = uptr + countSize + elementSize * length;
        var array = Array.CreateInstance(elementType, length);
        
        LogDebug($"Array length: {array.Length} at ptr {uptr}, array starting at {uptr+countSize}, ending at {ptrEnd} with element size {elementSize} | aa9d");
        
        // Marshalling element by element because in native AOT we're limited in what we can do
        // and System.Byte[] refused to marshal for me. Maybe there is a fix?
        for(var index = 0; index < length; index++)
        {
            var value = Marshal.PtrToStructure(currentPtr, elementType);
            array.SetValue(value, index);
            currentPtr += elementSize;
        }

        FreeUPtr(uptr);
        return array;
    }
    
    private static object FromNullableUPtr(IntPtr uptr, Type underlyingType)
    {
        var hasValue = Marshal.PtrToStructure<bool>(uptr);
        var nullablePtr =  uptr + NullableHasValuePrefixSize;
        var value = hasValue ? Marshal.PtrToStructure(nullablePtr, underlyingType) : null;
        
        FreeUPtr(uptr);
        return value;
    }
    
    private static Array FromNullableArrayUPtr(IntPtr uptr, Type underlyingType)
    {
        var elementSize = NullableHasValuePrefixSize;
        
        // not checking for blittable, let the framework deal with it
        const int countSize = 4;
        var underlyingElementSize = Marshal.SizeOf(underlyingType);
        elementSize += underlyingElementSize;
        var length = Marshal.PtrToStructure<int>(uptr);
        var currentPtr = uptr + countSize;
        var ptrEnd = uptr + countSize + elementSize * length;
        var array = Array.CreateInstance(underlyingType, length);

        if (DebugMode)
        {
            LogDebug($"Array length: {array.Length} at ptr {uptr}, array starting at {uptr + countSize}, ending at {ptrEnd} with element size {elementSize} for underlying type {underlyingType} | 283a");
            var bytes = new byte[(long)ptrEnd - (long)uptr];
            Marshal.Copy(uptr, bytes, 0, bytes.Length);
            LogDebug($"Bytes: {BitConverter.ToString(bytes).Replace("-", "")} | 283a");
        }

        // Marshalling element by element because in native AOT we're limited in what we can do
        for(var index = 0; index < length; index ++)
        {
            var hasValue = Marshal.PtrToStructure<bool>(currentPtr);
            currentPtr += NullableHasValuePrefixSize;
            if (hasValue)
            {
                var value = Marshal.PtrToStructure(currentPtr, underlyingType);
                array.SetValue(value, index);
            }

            currentPtr += underlyingElementSize;
        }
        
        FreeUPtr(uptr);
        return array;
    }
    
    
    #endregion

    /// <summary>
    /// Raises exception in the native library (python currently)
    /// </summary>
    /// <param name="ex">The exception to raise</param>
    public static void RaiseException(Exception ex)
    {
        LogDebug(ex.ToString());

        exceptionCallback(ex.GetType().FullName, ex.Message, GetTrimmedStackTrace(ex));
    }
    
    private static string GetTrimmedStackTrace(Exception ex)
    {
        var msg = ex.StackTrace;
        var lines = msg.Split(Environment.NewLine);
        var filtered = lines.Where(line => !line.Contains("at System.Runtime") && !line.Contains("End of stack trace from previous location")).ToList(); // excessive
        var dupeFiltered = new List<string>(filtered.Count);
        string prev = null;
        foreach (var line in filtered)
        {
            if (line == prev) continue;
            prev = line;
            dupeFiltered.Add(line);
        }

        var filteredMessage = string.Join(Environment.NewLine, dupeFiltered);
        return filteredMessage;
    }

    [UnmanagedCallersOnly(EntryPoint = "interoputils_set_exception_callback")]
    public static unsafe void SetExceptionCallback(delegate* unmanaged<IntPtr, IntPtr, IntPtr, void> callback)
    {
        InteropUtils.LogDebug($"Registering func handler {(IntPtr)callback} for interoputils_set_exception_callback");
        InteropUtils.exceptionCallback = (string type, string message, string trace) =>
        {
            var typePtr = InteropUtils.Utf8StringToUPtr(type);
            var messagePtr = InteropUtils.Utf8StringToUPtr(message);
            var tracePtr = InteropUtils.Utf8StringToUPtr(trace);
            InteropUtils.LogDebug($"Invoking handler {(IntPtr)callback} for interoputils_set_exception_callback");
            callback(typePtr, messagePtr, tracePtr);
        };
    }

    [UnmanagedCallersOnly(EntryPoint = "interoputils_log_debug")]
    public static void LogDebugInterop(IntPtr messagePtr)
    {
        if (messagePtr == IntPtr.Zero) return;
        var message = InteropUtils.PtrToStringUTF8(messagePtr);
        InteropUtils.LogDebug(message);
    }
    
    [UnmanagedCallersOnly(EntryPoint = "interoputils_log_debug_indentincr")]
    public static void LogDebugIndentIncrInterop()
    {
        LogDebugIndentIncr();
    }
    
    [UnmanagedCallersOnly(EntryPoint = "interoputils_log_debug_indentdecr")]
    public static void LogDebugIndentDecrInterop()
    {
        LogDebugIndentDecr();
    }
    
    
    [UnmanagedCallersOnly(EntryPoint = "interoputils_enabledebug")]
    public static void EnableDebug()
    {
        DebugMode = true;
    }
    
    [UnmanagedCallersOnly(EntryPoint = "interoputils_disabledebug")]
    public static void DisableDebug()
    {
        DebugMode = false;
    }
    
    [UnmanagedCallersOnly(EntryPoint = "interoputils_get_debug")]
    public static bool GetDebug()
    {
        return DebugMode;
    }

    [UnmanagedCallersOnly(EntryPoint = "interoputils_pin_hptr_target")]
    public static IntPtr PinHPtrTarget(IntPtr ptr)
    {
        LogDebug("Invoked interoputils_pin_hptr_target with HPtr: {0}", ptr);
        if (ptr == IntPtr.Zero)
        {
            if (DebugMode) Console.WriteLine($"Allocated null Ptr");
            return IntPtr.Zero;
        }
        var handler = GCHandle.FromIntPtr(ptr);
        var pinnedHandler = GCHandle.Alloc(handler.Target, GCHandleType.Pinned);
        var pptr = GCHandle.ToIntPtr(pinnedHandler);
        LogDebug("Allocated Pinned Ptr: {0}, type: {1}, {2}", pptr, handler.Target?.GetType().FullName, handler.Target == null ? "is null" : "is not null");
        return pptr;
    }
    
    [UnmanagedCallersOnly(EntryPoint = "interoputils_get_pin_address")]
    public static IntPtr GetPinAddress(IntPtr pinnedPtr)
    {
        LogDebug("Invoked interoputils_get_pin_address with Pinned Ptr: {0}", pinnedPtr);
        if (pinnedPtr == IntPtr.Zero) return IntPtr.Zero;
        var handler = GCHandle.FromIntPtr(pinnedPtr);
        return handler.AddrOfPinnedObject();
    }

    public static T FromHPtr<T>(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            LogDebug($"Converting null Ptr to type: {typeof(T).FullName}");
            return default;
        }
        var handler = GCHandle.FromIntPtr(ptr);
        LogDebug("Converted Ptr {0} to type: {1}, {2}", ptr, handler.Target?.GetType().FullName, handler.Target == null ? "is null" : "is not null");
        return (T)handler.Target;
    }
    
    /// <summary>
    /// Returns the pointer object
    /// </summary>
    /// <param name="ptr">The pointer</param>
    /// <returns>Null if zero pointer, else the object at the pointer</returns>
    public static object FromHPtr(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero) return null;
        var handler = GCHandle.FromIntPtr(ptr);
        return handler.Target;
    }

    public static object GetDefaultValue(Type type)
    {
        if (type.IsValueType) return Activator.CreateInstance(type);
        return null;
    }


    /// <summary>
    /// Free pinned <see cref="GCHandle"/> 
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "interoputils_free_hptr")]
    public static void FreeHPtr(IntPtr ptr)
    {
        LogDebug("Invoked interoputils_free_hptr with HPtr: {0}", ptr);
        if (ptr == IntPtr.Zero) return; // exception maybe ? (however that might be a problem due to unmanaged nature)
        var handler = GCHandle.FromIntPtr(ptr);
        LogDebug("Freed Ptr: {0}", ptr);
        handler.Free();
    }
    
    /// <summary>
    /// Free unmanaged memory pointer
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "interoputils_free_uptr")]
    public static void UnmanagedFreeUPtr(IntPtr ptr)
    {
        FreeUPtr(ptr);
    }
    
    public static void FreeUPtr(IntPtr ptr)
    {
        LogDebug("Invoked interoputils_free_uptr with UPtr: {0}", ptr);
        if (ptr == IntPtr.Zero) return; // exception maybe ? (however that might be a problem due to unmanaged nature)
        LogDebug("Freed UPtr: {0}", ptr);
        Marshal.FreeHGlobal(ptr);
    }
    
        
    /// <summary>
    /// Allocated unmanaged memory pointer of the desired size
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "interoputils_alloc_uptr")]
    public static IntPtr UnmanagedAllocateUPtr(int size)
    {
        return AllocateUPtr(size);
    }
    
    public static IntPtr AllocateUPtr(int size)
    {
        LogDebug("Allocating UPtr with size {0}", size);
        var ptr = Marshal.AllocHGlobal(size);
        LogDebug("Allocated UPtr: {0}, type: {1}, {2}", ptr, "UNMANAGED", "is not null");
        return ptr;
    }

    public static IntPtr Utf8StringToUPtr(in string str)
    {
        if (str == null) return IntPtr.Zero;
        var bytes = Encoding.UTF8.GetBytes(str);
        var pointer = Marshal.AllocHGlobal(bytes.Length + 1);  // +1 due to being null terminated string
        LogDebug("Allocated UPtr: {0}, type: {1}, {2}{3}", pointer, typeof(byte[]), "is not null", $",value {str}");
        Marshal.Copy(bytes, 0, pointer, bytes.Length);
        Marshal.WriteByte(pointer + bytes.Length, 0);
        return pointer;
    }

    public static string PtrToStringUTF8(IntPtr uptr, bool free = true)
    {
        if (uptr == IntPtr.Zero) return null;
        var res =  Marshal.PtrToStringUTF8(uptr);
        LogDebug("Converting UPtr->Str: {0}->{1}", uptr, res);
        if (free) FreeUPtr(uptr);
        return res;
    }
    
    public static IntPtr ObjectToPtr(object obj)
    {
        if (obj == null) return IntPtr.Zero;
        return ObjectToPtr(obj, obj.GetType());
    }

    public static IntPtr ObjectToPtr(object obj, Type typeHint)
    {
        if (obj == null) return IntPtr.Zero;
        return typeHint == typeof(string)
            ? InteropUtils.Utf8StringToUPtr((string)obj)
            : InteropUtils.ToHPtr(obj);
    }
    
    public static T PtrToObject<T>(IntPtr pointer)
    {
        return (T)PtrToObject(pointer, typeof(T));
    }

    public static object PtrToObject(IntPtr pointer, Type typeHint)
    {
        return typeHint == typeof(string)
            ? InteropUtils.PtrToStringUTF8(pointer)
            : InteropUtils.FromHPtr(pointer);
    }
    
    private static Dictionary<Type, bool> isBlittable = new Dictionary<Type, bool>();
    private static Action<string, string, string> exceptionCallback = (type, msg, trace) => { }; // do nothing by default

    public static bool IsBlittableType(Type type)
    {
        if (isBlittable.TryGetValue(type, out var blittable)) return blittable;
        blittable = IsBlittable(type);
        isBlittable[type] = blittable;
        return blittable;
    }
    
    // https://stackoverflow.com/a/68156592
    static bool IsBlittable(Type type)
        => IsBlittablePrimitive(type)
           || IsBlittableArray(type)
           || IsBlittableStruct(type)
           || IsBlittableClass(type);
    static bool IsBlittablePrimitive(Type type)
        => type == typeof(byte)
           || type == typeof(sbyte)
           || type == typeof(short)
           || type == typeof(ushort)
           || type == typeof(int)
           || type == typeof(uint)
           || type == typeof(long)
           || type == typeof(ulong)
           || type == typeof(System.IntPtr)
           || type == typeof(System.UIntPtr)
           || type == typeof(float)
           || type == typeof(double)
    ;
    static bool IsBlittableArray(Type type)
        => type.IsArray
           && type.GetArrayRank() == 1
           && IsBlittablePrimitive(type.GetElementType())
    ;
    static bool IsBlittableStruct(Type type)
        => type.IsValueType
           && !type.IsPrimitive
           && type.IsLayoutSequential
           && type.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).All(IsBlittableField);
    static bool IsBlittableClass(Type type)
        => !type.IsValueType
           && !type.IsPrimitive
           && type.IsLayoutSequential
           && type.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).All(IsBlittableField);
    static bool IsBlittableField(FieldInfo field)
        => IsBlittablePrimitive(field.FieldType) 
           || IsBlittableStruct(field.FieldType);
}