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

public class DictionaryInterop
{
    // This would be something such as IReadOnlyDictionary<,> which doesn't have a base type without generic

    /// <summary>
    /// Converts a dictionary GC handler pointer to an array of arrays with a length of 2. First element is the array
    /// of keys, second element is the array of values.
    /// </summary>
    [UnmanagedCallersOnly(EntryPoint = "dictionary_hptr_to_uptr")]
    public static IntPtr HPtrToUPtr(IntPtr dictionaryHPtr)
    {
        if (dictionaryHPtr == IntPtr.Zero) return IntPtr.Zero;
        try
        {
            // TODO check if could use ICollection ReadAny to get values
            // Every Dictionary type, even if read only implement IEnumerable.
            var target = InteropUtils.FromHPtr<IEnumerable>(dictionaryHPtr);
            var targetType = target.GetType();
            InteropUtils.LogDebug($"Converting HPtr {dictionaryHPtr} to {targetType}");

            if (target is IDictionary dictionary) return DictionaryToUptr(dictionary);

            // As the type is not guaranteed at this point, other than it has a Count property - likely from ICollection, use that info to see if empty
            var count = (int)targetType.GetProperty("Count").GetValue(target);
            if (count == 0)
            {
                return InteropUtils.ToUPtr(new IntPtr[]
                    { InteropUtils.ToUPtr(Array.Empty<IntPtr>()), InteropUtils.ToUPtr(Array.Empty<IntPtr>()) });
            }

            // Get the used types in the dictionary. (possibly rework for performance reasons?)
            var enumerator = target.GetEnumerator();
            enumerator.MoveNext();
            var first = enumerator.Current;

            var kpairType = first!.GetType(); // this can't be null, we expect KeyValuePair type here
            var kpairTypes = kpairType.GetGenericArguments();

            // Create and fill array for keys
            var (keyConverter, keyType) = EnumerableInterop.GetTypeConverterToUnmanaged(kpairTypes[0]);
            var targetKeys = (IEnumerable)targetType.GetProperty("Keys").GetValue(target);
            var keys = Array.CreateInstance(keyType, count);
            var keyEnumerator = targetKeys.GetEnumerator();
            var index = 0;
            while (keyEnumerator.MoveNext())
            {
                keys.SetValue(keyConverter(keyEnumerator.Current), index);
                index++;
            }

            // create and fill array for values
            var (valueConverter, valueType) = EnumerableInterop.GetTypeConverterToUnmanaged(kpairTypes[1]);
            var targetValues = (IEnumerable)targetType.GetProperty("Values").GetValue(target);
            var values = Array.CreateInstance(valueType, count);
            var valueEnumerator = targetValues.GetEnumerator();
            index = 0;
            while (valueEnumerator.MoveNext())
            {
                values.SetValue(valueConverter(valueEnumerator.Current), index);
                index++;
            }

            var kvals = InteropUtils.ToArrayUPtr(keys, keyType);
            var vvals = InteropUtils.ToArrayUPtr(values, valueType);
            // Create return array, which is always [keys[], values[]]
            var resultArr = new IntPtr[] { kvals, vvals };
            return InteropUtils.ToUPtr(resultArr);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug($"Exception in dictionary_hptr_to_uptr");
            InteropUtils.LogDebug($"Arg dictionaryHPtr (IntPtr) has value: {dictionaryHPtr}");
            InteropUtils.RaiseException(ex);
            return default;
        }
    }

    private static IntPtr DictionaryToUptr(IDictionary dictionary)
    {
        var count = dictionary.Count;
        if (count == 0)
        {
            return InteropUtils.ToUPtr(new IntPtr[]
                { InteropUtils.ToUPtr(Array.Empty<IntPtr>()), InteropUtils.ToUPtr(Array.Empty<IntPtr>()) });
        }

        // Get the used types in the dictionary. (possibly rework for performance reasons?)
        var keys = CollectionInterop.ToUPtr(dictionary.Keys);
        var vals = CollectionInterop.ToUPtr(dictionary.Values);
        var resultArr = new IntPtr[] { keys, vals };
        return InteropUtils.ToUPtr(resultArr);
    }
    
    /// <summary>
    /// Converts a dictionary which is an array of length 2 of keys and values to a managed dictionary
    /// <see cref="HPtrToUPtr"/> for the opposite way
    /// </summary>
    internal static TDictionary FromUPtr<TDictionary, TKey, TValue>(IntPtr dictionaryUPtr) where TDictionary : class, IDictionary, new()
    {
        InteropUtils.LogDebug($"Converting Dictionary<{typeof(TKey)},{typeof(TValue)}> with ptr {dictionaryUPtr}");
        if (dictionaryUPtr == IntPtr.Zero) return null;

        var dict = new TDictionary();

        var keysvals = (IntPtr[])InteropUtils.FromArrayUPtr(dictionaryUPtr, typeof(IntPtr));
        var keysUptr = keysvals[0];
        var valuesUptr = keysvals[1];
        var (keyConverter, keyType) = EnumerableInterop.GetTypeConverterFromUnmanaged(typeof(TKey));
        var (valueConverter, valueType) = EnumerableInterop.GetTypeConverterFromUnmanaged(typeof(TValue));
        var keys = InteropUtils.FromArrayUPtr(keysUptr, keyType);
        var values = InteropUtils.FromArrayUPtr(valuesUptr, valueType);
        for (int ii = 0; ii < keys.Length; ii++)
        {
            var key = keys.GetValue(ii);
            var value = values.GetValue(ii);
            var keyConverted = keyConverter(key);
            var valueConverted = valueConverter(value);
            dict.Add(keyConverted, valueConverted);
        }
        
        return dict;
    }
    
    # region HPtr helpers, useful when must maintain a managed reference
    
    /// <summary>
    /// Creates a new Dict[string,string] and returns a gc handler pointer for it
    /// </summary>
    /// <returns>gc handler pointer for it</returns>
    [UnmanagedCallersOnly(EntryPoint = "dictionary_constructor_string_string")]
    public static IntPtr ConstructorForStringString()
    {
        var dict = new Dictionary<string,string>();
        return InteropUtils.ToHPtr(dict);
    }
    
    [UnmanagedCallersOnly(EntryPoint = "dictionary_clear")]
    public static void Clear(IntPtr dictionaryHPtr)
    {
        try
        {
            var target = InteropUtils.FromHPtr<IDictionary>(dictionaryHPtr);
            target.Clear();
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug($"Exception in dictionary_clear");
            InteropUtils.LogDebug($"Arg dictionaryHPtr (IntPtr) has value: {dictionaryHPtr}");
            InteropUtils.RaiseException(ex);
        }
    }
    
    [UnmanagedCallersOnly(EntryPoint = "dictionary_remove")]
    public static void Remove(IntPtr dictionaryHPtr, IntPtr keyHPtr)
    {
        try
        {
            var target = InteropUtils.FromHPtr<IDictionary>(dictionaryHPtr);
            var keyType = target.GetType().GetGenericArguments()[0];

            object key = InteropUtils.PtrToObject(keyHPtr, keyType);

            target.Remove(key);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in dictionary_remove");
            InteropUtils.LogDebug($"Arg dictionaryHPtr (IntPtr) has value: {dictionaryHPtr}");
            InteropUtils.LogDebug($"Arg keyHPtr (IntPtr) has value: {keyHPtr}");
            InteropUtils.RaiseException(ex);
        }
    }
    
    [UnmanagedCallersOnly(EntryPoint = "dictionary_get_count")]
    public static int GetCount(IntPtr dictionaryHPtr)
    {
        try
        {
            var target = InteropUtils.FromHPtr<IDictionary>(dictionaryHPtr);
            return target.Count;
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in dictionary_get_count");
            InteropUtils.LogDebug($"Arg dictionaryHPtr (IntPtr) has value: {dictionaryHPtr}");
            InteropUtils.RaiseException(ex);
            return default;
        }
    }
    
    [UnmanagedCallersOnly(EntryPoint = "dictionary_set_value")]
    public static void SetValue(IntPtr dictionaryHPtr, IntPtr keyHPtr, IntPtr valHPtr)
    {
        object value = null;
        object key = null;
        Type keyType = null;
        Type valType = null;
        Type dictType = null;
        try
        {
            var target = InteropUtils.FromHPtr<IDictionary>(dictionaryHPtr);
            var targetType = target.GetType();
            dictType = (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(IDictionary<,>))
                ? targetType
                : targetType.GetInterfaces().FirstOrDefault(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IDictionary<,>));
            if (dictType == null)
            {
                throw new InvalidOperationException($"Type {targetType} does not implement dictionary with types nor is a dictionary with types");
            }
            keyType = dictType.GetGenericArguments()[0];
            valType = dictType.GetGenericArguments()[1];

            value = InteropUtils.PtrToObject(valHPtr, valType);

            key = InteropUtils.PtrToObject(keyHPtr, keyType);

            target[key] = value;
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in dictionary_set_value");
            InteropUtils.LogDebug($"Arg dictionaryHPtr (IntPtr) has value: {dictionaryHPtr}, converted to dict type {dictType}");
            InteropUtils.LogDebug($"Arg keyHPtr (IntPtr) has value: {keyHPtr}, type {keyType}, converted {key}");
            InteropUtils.LogDebug($"Arg valHPtr (IntPtr) has value: {valHPtr}, type {valType}, converted {value}");
            InteropUtils.RaiseException(ex);
        }
    }
    
    [UnmanagedCallersOnly(EntryPoint = "dictionary_get_value")]
    public static IntPtr GetValue(IntPtr dictionaryHPtr, IntPtr keyHPtr)
    {
        object value = null;
        object key = null;
        Type keyType = null;
        Type valType = null;
        Type dictType = null;
        try
        {
            var target = InteropUtils.FromHPtr<IDictionary>(dictionaryHPtr);
            var targetType = target.GetType();
            dictType = (targetType.IsGenericType && targetType.GetGenericTypeDefinition() == typeof(IDictionary<,>))
                ? targetType
                : targetType.GetInterfaces().FirstOrDefault(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IDictionary<,>));
            if (dictType == null)
            {
                throw new InvalidOperationException($"Type {targetType} does not implement dictionary with types nor is a dictionary with types");
            }
            keyType = dictType.GetGenericArguments()[0];
            valType = dictType.GetGenericArguments()[1];

            key = InteropUtils.PtrToObject(keyHPtr, keyType);

            value = target[key];

            return InteropUtils.ObjectToPtr(value, valType);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug("Exception in dictionary_get_value");
            InteropUtils.LogDebug($"Arg dictionaryHPtr (IntPtr) has value: {dictionaryHPtr}, converted to dict type {dictType}");
            InteropUtils.LogDebug($"Arg keyHPtr (IntPtr) has value: {keyHPtr}, type {keyType}, converted {key}");
            InteropUtils.LogDebug($"Value is type {valType}, with value of {value}");
            InteropUtils.RaiseException(ex);
            return default;
        }
    }
    
    [UnmanagedCallersOnly(EntryPoint = "dictionary_get_keys")]
    public static IntPtr GetKeys(IntPtr dictionaryHPtr)
    {
        try
        {
            var target = InteropUtils.FromHPtr<IDictionary>(dictionaryHPtr);
            // I couldn't justify returning it as HPtr, given this is not meant to be a modifiable list
            // Individual elements can still be modified once converted back to objects using their pointer
            // if they were references
            return CollectionInterop.ToUPtr(target.Keys);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug($"Exception in dictionary_get_keys");
            InteropUtils.LogDebug($"Arg dictionaryHPtr (IntPtr) has value: {dictionaryHPtr}");
            InteropUtils.RaiseException(ex);
            return default;
        }
    }

    [UnmanagedCallersOnly(EntryPoint = "dictionary_get_values")]
    public static IntPtr GetValues(IntPtr dictionaryHPtr)
    {
        try
        {
            var target = InteropUtils.FromHPtr<IDictionary>(dictionaryHPtr);
            // I couldn't justify returning it as HPtr, given this is not meant to be a modifiable list
            // Individual elements can still be modified once converted back to objects using their pointer
            // if they were references
            return CollectionInterop.ToUPtr(target.Values);
        }
        catch (Exception ex)
        {
            InteropUtils.LogDebug($"Exception in dictionary_get_values");
            InteropUtils.LogDebug($"Arg dictionaryHPtr (IntPtr) has value: {dictionaryHPtr}");
            InteropUtils.RaiseException(ex);
            return default;
        }
    }
    
    # endregion
}