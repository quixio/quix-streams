// ***********************GENERATED CODE WARNING************************
// This file is code generated, any modification you do will be lost the
// next time this file is regenerated.
// *********************************************************************

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace InteropHelpers.Interop.ExternalTypes.System;

public class EnumerableInterop
{
    
    /// <summary>
    /// Retrieves the necessary conversion function to convert to unmanaged type
    /// </summary>
    /// <param name="type">The type to convert to unmanaged</param>
    /// <returns>Tuple of conversion function and unmanaged type it results in</returns>
    /// /// <seealso cref="GetTypeConverterFromUnmanaged"/>
    public static (Func<object, object>, Type) GetTypeConverterToUnmanaged(Type type)
    {
        if (type == typeof(string))
        {
            InteropUtils.LogDebug($"{nameof(GetTypeConverterToUnmanaged)}-{type}->StringPointer");
            return (y =>
            {
                return InteropUtils.Utf8StringToUPtr((string)y);
            }, typeof(IntPtr));
        }

        if (typeof(Array).IsAssignableFrom(type))
        {
            InteropUtils.LogDebug($"{nameof(GetTypeConverterToUnmanaged)}-{type}->UPtr (array)");
            var elementType = type.GetElementType();

            var (elementCallback, elementConvertedType) = GetTypeConverterToUnmanaged(elementType);
            return (y =>
            {
                InteropUtils.LogDebug($"{nameof(GetTypeConverterToUnmanaged)} 1");
                var array = (Array)y;
                InteropUtils.LogDebug($"{nameof(GetTypeConverterToUnmanaged)} 2");
                var convertedElements = Array.CreateInstance(elementConvertedType, array.Length);
                InteropUtils.LogDebug($"{nameof(GetTypeConverterToUnmanaged)} 3 type {elementType}");
                for (int ii = 0; ii < array.Length; ii++)
                {
                    convertedElements.SetValue(elementCallback(array.GetValue(ii)), ii);
                }
                InteropUtils.LogDebug($"{nameof(GetTypeConverterToUnmanaged)} 4 {array.Length}");
                return InteropUtils.ToArrayUPtr(convertedElements, elementConvertedType);
            }, typeof(IntPtr));
        }

        if (!InteropUtils.IsBlittableType(type))
        {
            InteropUtils.LogDebug($"{nameof(GetTypeConverterToUnmanaged)}-{type}->HPtr");
            return (y => InteropUtils.ToHPtr(y), typeof(IntPtr));
        }

        InteropUtils.LogDebug($"{nameof(GetTypeConverterToUnmanaged)}-{type}->{type}");
        return (y => y, type);
    }
    
    
    /// <summary>
    /// Retrieves the necessary conversion function to convert from unmanaged type
    /// </summary>
    /// <param name="type">The type expected to have after conversion</param>
    /// <returns>Tuple of conversion function and unmanaged type it has to be applied to</returns>
    /// <seealso cref="GetTypeConverterToUnmanaged"/>
    public static (Func<object, object>, Type) GetTypeConverterFromUnmanaged(Type type)
    {
        if (type == typeof(string))
        {
            InteropUtils.LogDebug($"{nameof(GetTypeConverterFromUnmanaged)}-StringPointer->{type}");
            return (y =>
            {
                //Console.WriteLine($"GetTypeConverter-{type}->StringPointer, str: {y}");
                return InteropUtils.PtrToStringUTF8((IntPtr)y);
            }, typeof(IntPtr));
        }
        
        if (typeof(Array).IsAssignableFrom(type))
        {
            InteropUtils.LogDebug($"{nameof(GetTypeConverterFromUnmanaged)}->UPtr->{type}");
            var elementType = type.GetElementType();
            var (elementCallback, elementConvertedType) = GetTypeConverterFromUnmanaged(elementType);
            return (y =>
            {
                var array = InteropUtils.FromArrayUPtr((IntPtr)y, elementConvertedType);
                var convertedElements = Array.CreateInstance(elementType, array.Length);
                for (int ii = 0; ii < array.Length; ii++)
                {
                    convertedElements.SetValue(elementCallback(array.GetValue(ii)), ii);
                }

                return convertedElements;
            }, typeof(IntPtr));
        }

        if (!InteropUtils.IsBlittableType(type))
        {
            InteropUtils.LogDebug($"{nameof(GetTypeConverterFromUnmanaged)}->HPtr->{type}");
            return (y => InteropUtils.ToHPtr(y), typeof(IntPtr));
        }

        InteropUtils.LogDebug($"{nameof(GetTypeConverterFromUnmanaged)}-{type}->{type}");
        return (y => y, type);
    }
    
    [UnmanagedCallersOnly(EntryPoint = "enumerable_read_any")]
    public static IntPtr ReadAny(IntPtr arrayPtr)
    {
        try
        {
            var enumerable = InteropUtils.FromHPtr<IEnumerable>(arrayPtr);

            if (enumerable is ICollection collection)
            {
                return CollectionInterop.ToUPtr(collection);
            }

            var enumerator = enumerable.GetEnumerator();
            if (!enumerator.MoveNext()) InteropUtils.ToHPtr(Array.Empty<IntPtr>());
            int nullCounter = 0;
            while (enumerator.Current == null)
            {
                nullCounter++;
                if (!enumerator.MoveNext())
                {
                    var nullArray = new IntPtr[nullCounter];
                    return InteropUtils.ToHPtr(nullArray);
                }
            }

            var valueType = enumerator.Current.GetType();
            var (converter, actualType) = EnumerableInterop.GetTypeConverterToUnmanaged(valueType);

            var values = (IList)Activator.CreateInstance(typeof(List<>)
                        .MakeGenericType(actualType)); // Maybe rework to multiple array, so less type dependency?
            values.Insert(nullCounter, converter(enumerator.Current));
            var index = nullCounter + 1;
            while (enumerator.MoveNext())
            {
                values.Insert(index, converter(enumerator.Current));
                index++;
            }

            return InteropUtils.ToHPtr(values);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug($"Exception in enumerable_read_any");
            InteropUtils.RaiseException(ex);
            return default;
        }
    }
    
    [UnmanagedCallersOnly(EntryPoint = "enumerable_read_strings")]
    public static IntPtr ReadStrings(IntPtr arrayPtr)
    {
        try
        {
            var enumerable = InteropUtils.FromHPtr<IEnumerable<string>>(arrayPtr);

            if (enumerable is ICollection<string> collection)
            {
                return CollectionInterop.ToUPtrString(collection);
            }

            var converted = enumerable.Select(y => InteropUtils.Utf8StringToUPtr(y)).ToArray();

            return InteropUtils.ToUPtr(converted);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug($"Exception in enumerable_read_strings");
            InteropUtils.RaiseException(ex);
            return default;
        }
    }


    [UnmanagedCallersOnly(EntryPoint = "enumerable_read_references")]
    public static IntPtr ReadReferences(IntPtr arrayPtr)
    {
        try
        {
            var enumerable = InteropUtils.FromHPtr<IEnumerable>(arrayPtr);

            if (enumerable is ICollection collection)
            {
                return CollectionInterop.ReferencesToUPtr(collection);
            }

            var convertedList = new List<IntPtr>();

            var enumerator = enumerable.GetEnumerator();
            while (enumerator.MoveNext())
            {
                convertedList.Add(InteropUtils.ToHPtr(enumerator.Current));
            }

            var converted = convertedList.ToArray();

            return InteropUtils.ToUPtr(converted);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug($"Exception in enumerable_read_references");
            InteropUtils.RaiseException(ex);
            return default;
        }
    }
}