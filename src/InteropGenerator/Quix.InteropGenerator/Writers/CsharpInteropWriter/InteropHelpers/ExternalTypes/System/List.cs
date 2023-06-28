// ***********************GENERATED CODE WARNING************************
// This file is code generated, any modification you do will be lost the
// next time this file is regenerated.
// *********************************************************************

using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace InteropHelpers.Interop.ExternalTypes.System;

public class ListInterop
{
    [UnmanagedCallersOnly(EntryPoint = "list_constructor_string")]
    public static IntPtr ConstructorForString()
    {
        try
        {
            var list = new List<string>();
            return InteropUtils.ToHPtr(list);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in list_constructor_string");
            InteropUtils.RaiseException(ex);
            return default;
        }
    }

    [UnmanagedCallersOnly(EntryPoint = "list_get_value")]
    public static IntPtr GetValue(IntPtr dictionaryHPtr, int index)
    {
        try
        {
            var target = InteropUtils.FromHPtr<IList>(dictionaryHPtr);
            var value = target[index];
            return InteropUtils.ObjectToPtr(value);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in list_get_value");
            InteropUtils.RaiseException(ex);
            return default;
        }
    }

    [UnmanagedCallersOnly(EntryPoint = "list_contains")]
    public static bool Contains(IntPtr dictionaryHPtr, IntPtr valuePtr)
    {
        try
        {
            var target = InteropUtils.FromHPtr<IList>(dictionaryHPtr);
            var listType = target.GetType().GetGenericArguments()[0];
            var value = InteropUtils.PtrToObject(valuePtr, listType);
            return target.Contains(value);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in list_contains");
            InteropUtils.RaiseException(ex);
            return default;
        }
    }

    [UnmanagedCallersOnly(EntryPoint = "list_set_at")]
    public static void SetAt(IntPtr dictionaryHPtr, int index, IntPtr valuePtr)
    {
        try
        {
            var target = InteropUtils.FromHPtr<IList>(dictionaryHPtr);
            var listType = target.GetType().GetGenericArguments()[0];
            var value = InteropUtils.PtrToObject(valuePtr, listType);
            target[index] = value;
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in list_set_at");
            InteropUtils.RaiseException(ex);
        }
    }

    [UnmanagedCallersOnly(EntryPoint = "list_remove_at")]
    public static void RemoveAt(IntPtr dictionaryHPtr, int index)
    {
        try
        {
            var target = InteropUtils.FromHPtr<IList>(dictionaryHPtr);
            target.RemoveAt(index);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in list_remove_at");
            InteropUtils.RaiseException(ex);
        }
    }

    [UnmanagedCallersOnly(EntryPoint = "list_remove")]
    public static void Remove(IntPtr dictionaryHPtr, IntPtr valuePtr)
    {
        try
        {
            var target = InteropUtils.FromHPtr<IList>(dictionaryHPtr);
            var listType = target.GetType().GetGenericArguments()[0];
            var value = InteropUtils.PtrToObject(valuePtr, listType);
            target.Remove(value);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in list_remove");
            InteropUtils.RaiseException(ex);
        }
    }

    [UnmanagedCallersOnly(EntryPoint = "list_add")]
    public static void Add(IntPtr dictionaryHPtr, IntPtr valuePtr)
    {
        try
        {
            var target = InteropUtils.FromHPtr<IList>(dictionaryHPtr);
            var listType = target.GetType().GetGenericArguments()[0];
            var value = InteropUtils.PtrToObject(valuePtr, listType);
            target.Add(value);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in list_add");
            InteropUtils.RaiseException(ex);
        }
    }

    [UnmanagedCallersOnly(EntryPoint = "list_clear")]
    public static void Clear(IntPtr dictionaryHPtr)
    {
        try
        {
            var target = InteropUtils.FromHPtr<IList>(dictionaryHPtr);
            target.Clear();
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in list_clear");
            InteropUtils.RaiseException(ex);
        }
    }
}