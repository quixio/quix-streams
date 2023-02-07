// ***********************GENERATED CODE WARNING************************
// This file is code generated, any modification you do will be lost the
// next time this file is regenerated.
// *********************************************************************

using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace InteropHelpers.Interop.ExternalTypes.System;

public class CollectionInterop
{
    /// <summary>
    /// Converts a collection to unmanaged memory. Non-Blittable types convert to pointers, while blittable
    /// is used with its binary value
    /// </summary>
    /// <param name="collection"></param>
    /// <returns></returns>
    public static IntPtr ToUPtr(ICollection collection)
    {
        InteropUtils.LogDebug($"Converting ICollection {collection.GetType()}");
        var count = collection.Count;
        if (count <= 0) return InteropUtils.ToArrayUPtr(Array.Empty<IntPtr>(), typeof(IntPtr));
        var enumerator = collection.GetEnumerator();
        if (!enumerator.MoveNext()) return InteropUtils.ToArrayUPtr(Array.Empty<IntPtr>(), typeof(IntPtr));
        
        // Check if full of null
        int nullCounter = 0;
        while (enumerator.Current == null)
        {
            nullCounter++;
            if (!enumerator.MoveNext())
            {
                var nullArray = new IntPtr[nullCounter];
                return InteropUtils.ToArrayUPtr(nullArray, typeof(IntPtr));
            }
        }

        var valueType = enumerator.Current.GetType();
        var (converter, actualType) = EnumerableInterop.GetTypeConverterToUnmanaged(valueType);
        InteropUtils.LogDebug($"Converting to {actualType}");

        var values = Array.CreateInstance(actualType, count);
        values.SetValue(converter(enumerator.Current), nullCounter);
        var index = nullCounter + 1;
        while (enumerator.MoveNext())
        {
            values.SetValue(converter(enumerator.Current), index);
            index++;
        }
        
        return InteropUtils.ToArrayUPtr(values, actualType);
    }
    
    /// <summary>
    /// More efficient version of the generic <see cref="ToUPtr(System.Collections.ICollection)"/> when type is known
    /// to be string
    /// </summary>
    public static IntPtr ToUPtrString(ICollection<string> collection)
    {
        var count = collection.Count;
        if (count <= 0) return InteropUtils.ToArrayUPtr(Array.Empty<IntPtr>(), typeof(IntPtr));
        var converted = new IntPtr[count];
        var ii = 0;
        foreach (var e in collection)
        {
            converted[ii] = InteropUtils.Utf8StringToUPtr(e);
            ii++;
        }

        return InteropUtils.ToUPtr(converted);
    }

    /// <summary>
    /// More efficient version of the generic <see cref="ToUPtr(System.Collections.ICollection)"/> when type is known
    /// to be of reference type
    /// </summary>
    public static IntPtr ReferencesToUPtr(ICollection collection)
    {
        var count = collection.Count;
        if (count <= 0) return InteropUtils.ToArrayUPtr(Array.Empty<IntPtr>(), typeof(IntPtr));
        var converted = new IntPtr[count];
        var ii = 0;
        foreach (var e in collection)
        {
            converted[ii] = InteropUtils.ToHPtr(e);
            ii++;
        }

        return InteropUtils.ToUPtr(converted);
    }
    
    [UnmanagedCallersOnly(EntryPoint = "collection_get_count")]
    public static int GetCount(IntPtr collectionHPtr)
    {
        try
        {
            var target = InteropUtils.FromHPtr<ICollection>(collectionHPtr);
            return target.Count;
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in collection_get_count");
            InteropUtils.RaiseException(ex);
            return default;
        }
    }
}