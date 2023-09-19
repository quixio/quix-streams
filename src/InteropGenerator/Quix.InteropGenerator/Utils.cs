using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.Logging;

namespace Quix.InteropGenerator;

public class Utils
{
    private static Dictionary<Type, bool> isBlittable = new Dictionary<Type, bool>();
    private static ILogger logger = Logger.LoggerFactory.CreateLogger<Utils>();

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

    public static bool HasParameterlessConstructor(Type type) =>
        type.GetConstructors().FirstOrDefault(y => y.IsConstructor && y.GetParameters().Length == 0) != null;
    
    public static bool HasEvents(Type type) => type.GetEvents().Any();
     
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
           && type.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).All(IsBlittableField)
           && HasParameterlessConstructor(type)
           && !HasEvents(type); 
    static bool IsBlittableField(FieldInfo field)
        => IsBlittablePrimitive(field.FieldType) 
           || IsBlittableStruct(field.FieldType);

    public static IList<MethodBase> GetWrappableMethodInfos(Type type)
    {
        var methodBases = new List<MethodBase>();

        if (!type.IsAbstract)
        {
            var constructors = type.GetConstructors().Where(y => y.IsPublic).ToList();
            foreach (var constructor in constructors)
            {
                if (!IsInteropSupported(constructor)) continue;
                methodBases.Add(constructor);
            }
        }

        var methodInfos = type.GetMethods().Where(y => y.IsPublic && (y.DeclaringType.IsInterface || y.DeclaringType == type || y.DeclaringType.IsAbstract)).ToList();

        foreach (var methodInfo in methodInfos)
        {
            // The type will not be inferrable without some magic of replacing the method signature with the type it is getting created for
            if (!IsInteropSupported(methodInfo)) continue;
            methodBases.Add(methodInfo);
        }

        return methodBases;
    }

    public static IEnumerable<Type> GetUsedTypes(Type type, bool includeOriginal = true)
    {
        if (includeOriginal) yield return type;
        if (Utils.IsMulticastDelegate(type))
        {
            var mi = Utils.GetMulticastDelegateMethodInfo(type);
            foreach (var param in mi.GetParameters())
            {
                yield return param.ParameterType;
            }

            yield return mi.ReturnType;
            yield break;

        }
        if (type.IsGenericType)
        {
            foreach (var argType in type.GenericTypeArguments)
            {
                yield return argType;
            }
        }
    }
    
    /// <summary>
    /// Returns the types used as parameter or return type by the method
    /// </summary>
    /// <param name="methodBase"></param>
    /// <returns></returns>
    public static IEnumerable<Type> GetUsedTypes(Type declaringType, MethodBase methodBase)
    {
        IEnumerable<Type> GetTypes()
        {
            var parameters = methodBase.GetParameters();

            foreach (var parameterInfo in parameters)
            {
                foreach (var type in GetUsedTypes(parameterInfo.ParameterType))
                {
                    yield return type;
                }
            }

            if (methodBase is MethodInfo mi)
            {
                yield return mi.ReturnType;
            }

            if (methodBase is ConstructorInfo ci)
            {
                yield return declaringType;
            }
        }

        return GetTypes().Distinct();
    }

    public static MethodInfo GetMulticastDelegateMethodInfo(Type multicastDelegateType)
    {
        if (!IsMulticastDelegate(multicastDelegateType)) throw new ArgumentException($"{multicastDelegateType.FullName} is not MultiCastDelegate");
        var invokeMethod = multicastDelegateType.GetMethod("Invoke");
        return invokeMethod;
    }

    public static List<Type> UnmanagedTypes = new List<Type>()
    {
        typeof(void),
        typeof(IntPtr),
        typeof(UIntPtr),
        typeof(Byte),
        typeof(SByte),
        typeof(short),
        typeof(ushort),
        typeof(int),
        typeof(uint),
        typeof(long),
        typeof(ulong),
        typeof(float),
        typeof(double)
    };

    private static readonly List<Type> PythonMappableTypes = new List<Type>()
    {
        typeof(string)
    }.Union(UnmanagedTypes).ToList();

    public static Type GetUnmanagedType(Type type)
    {
        if (UnmanagedTypes.Contains(type)) return type;
        if (Nullable.GetUnderlyingType(type) != null)
        {
            var underlyingType = GetUnmanagedType(Nullable.GetUnderlyingType(type));
            if (underlyingType == typeof(IntPtr)) return underlyingType; // Pointers don't do well with nullable wrapper
            return typeof(Nullable<>).MakeGenericType(underlyingType);
        }
        if (type == typeof(bool)) return typeof(byte);
        if (Utils.IsEnum(type)) return type;
        if (Utils.IsDelegate(type))
        {
            //var mi = GetMulticastDelegateMethodInfo(type);
            //var returnType = Type.MakeGenericSignatureType(typeof(MulticastDelegate), mi.GetParameters().Select(y=> GetUnmanagedType(y.ParameterType)).ToArray());
            //            return returnType; // Causes exception elsewhere due to not supporting basetype
            return typeof(Delegate); // this is essentially just a marker. The actual code depends on the original type
        }

        // if (IsBlittable(type) && !IsBlittableArray(type)) return type; // Not yet implemented correctly
        return typeof(IntPtr);
    }
    
    /// <summary>
    /// Converts the type to a type that can be used for python wrapping.
    /// It doesn't return a python type, but a c# type that can be mapped to a python type easily
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    public static Type GetPythonMappableType(Type type)
    {
        if (PythonMappableTypes.Contains(type)) return type;
        if (Nullable.GetUnderlyingType(type) != null)
        {
            var underlyingType = GetPythonMappableType(Nullable.GetUnderlyingType(type));
            if (underlyingType == typeof(IntPtr)) return underlyingType; // Pointers don't do well with nullable wrapper
            return typeof(Nullable<>).MakeGenericType(underlyingType);
        }
        if (type == typeof(bool)) return typeof(bool);
        if (Utils.IsEnum(type)) return type;
        if (Utils.IsDelegate(type))
        {
            //var mi = GetMulticastDelegateMethodInfo(type);
            //var returnType = Type.MakeGenericSignatureType(typeof(Delegate), mi.GetParameters().Select(y=> y.ParameterType).ToArray());
            //            return returnType;  // Causes exception elsewhere due to not supporting basetype
            return typeof(Delegate); // this is essentially just a marker. The actual code depends on the original type
        }
        
        if (type.Assembly.GetName().Name == "Extra") return type;
        return typeof(IntPtr);
    }
    
    /// <summary>
    /// Returns the relative path the type is expected to be defined at from its assembly root
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    internal static string GetTypePath(Type type)
    {
        // Expected result here:
        // 1) type of A.B.C in namespace A.B, Assembly A.B = C
        // 2) type of A.B.C in namespace A.B, Assembly X.Y = A.B.C
        // 3) type of A.B.C in namespace A.B, Assembly A.B.C = C
        var typePath = type.FullName;
        var assemblyNameForReplace = type.Assembly.GetName().Name;
        if (!type.Namespace.StartsWith(assemblyNameForReplace))
        {
            if (type.FullName == assemblyNameForReplace)
            {
                typePath = typePath.Replace(type.Namespace, "");
            }
        }
        else
        {
            typePath = typePath.Replace(assemblyNameForReplace, "");
        }
        
        return typePath.Trim('.').Replace("+", "_").Replace('.', Path.DirectorySeparatorChar);
    }
    
    
    
    /// <summary>
    /// Returns the name of the argument that will reference to the instance the method should be invoked with
    /// </summary>
    /// <param name="methodBase"></param>
    /// <returns></returns>
    public static string GetInstanceParameterName(Type declaringType, MethodBase methodBase)
    {
        if (methodBase.IsStatic) return null;
        if (methodBase.IsConstructor) return null;
        var typeNameForNaming = GetTypeNameForNaming(declaringType);
        var name = typeNameForNaming.Substring(0, 1).ToLowerInvariant() + typeNameForNaming.Substring(1);
        var paramName = name;
        var counter = 0;
        while (methodBase.GetParameters().Any(y => y.Name == paramName))
        {
            counter++;
            paramName = name + counter;

        }

        return paramName;
    }

    private static Dictionary<string, string> NameToTypeDictionary { get; } = new Dictionary<string, string>();
    private static Dictionary<string, string> TypeToNameDictionary { get; } = new Dictionary<string, string>();

    public static string GetTypeNameForNaming(Type type)
    {
        var key = type.FullName ?? throw new NotSupportedException($"{type} has no full name");
        if (type.IsGenericType) throw new NotSupportedException("Generics are not supported.");
        if (TypeToNameDictionary.TryGetValue(key, out var name)) return name;

        int suffix = 1;
        var canCheckRealType = true;
        name = type.Name;
        while (true)
        {
            name = suffix == 1 ? type.Name : $"{type.Name}{suffix}";
            if (NameToTypeDictionary.TryGetValue(name, out var existingMap))
            {
                suffix++;
                continue;
            }
            
            TypeToNameDictionary[key] = name;
            NameToTypeDictionary[name] = key;
            return name;
        }
    }

    public static Type GetReplacementType(Type type)
    {
        if (type == typeof(Span<char>))
        {
            return typeof(string);
        }
        if (type == typeof(ReadOnlySpan<char>))
        {
            return typeof(string);
        }
        
        return type;
    }
    /// <summary>
    /// Determines whether the specified type is a delegate or a subclass of Delegate.
    /// </summary>
    /// <param name="type">The Type object to check.</param>
    /// <returns>true if the type is a delegate or a subclass of Delegate; otherwise, false.</returns>
    public static bool IsDelegate(Type type)
    {
        return typeof(Delegate).IsAssignableFrom(type);
    }

    /// <summary>
    /// Determines whether the specified type is exactly equal to the Delegate or Action Class
    /// </summary>
    /// <param name="type">The Type object to check.</param>
    /// <returns>true if the type is exactly equal to the Delegate class; otherwise, false.</returns>
    public static bool IsDelegateType(Type type)
    {
        return type == typeof(Delegate) || type == typeof(Action);
    }

    /// <summary>
    /// Determines whether the specified type is a subclass of the MulticastDelegate class.
    /// </summary>
    /// <param name="type">The Type object to check.</param>
    /// <returns>true if the type is a subclass of the MulticastDelegate class; otherwise, false.</returns>
    public static bool IsMulticastDelegate(Type type)
    {
        return type.BaseType == typeof(MulticastDelegate);
    }


    public static bool IsEnum(Type type)
    {
        return type != typeof(Enum) && typeof(Enum).IsAssignableFrom(type);
    }

    public static bool IsException(Type type)
    {
        return typeof(Exception).IsAssignableFrom(type);
    }
    
    /// <summary>
    /// Returns the types that the type declaration uses
    /// Ex: List[T] would return List'1 and T
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    public static IEnumerable<Type> GetUnderlyingTypes(Type type)
    {
        if (type.IsArray || type.IsByRef)
        {
            foreach (var underlyingType in GetUnderlyingTypes(type.GetElementType()))
            {
                yield return underlyingType;
            }

            yield break;
        }
        
        if (Utils.IsMulticastDelegate(type))
        {
            var mi = Utils.GetMulticastDelegateMethodInfo(type);
            foreach (var param in mi.GetParameters())
            {
                yield return param.ParameterType;
            }

            yield return mi.ReturnType;
            yield return type;
            yield break;

        }
            
        if (!type.IsGenericType)
        {
            yield return type;
            yield break;
        }

        yield return type.GetGenericTypeDefinition();

        foreach (var subType in type.GetGenericArguments())
        {
            foreach (var underlyingType in GetUnderlyingTypes(subType))
            {
                yield return underlyingType;
            }
        }
    }

    private static Dictionary<Type, bool> isInteropSupportedType = new Dictionary<Type, bool>();

    public static bool IsInteropSupported(Type type)
    {
        if (isInteropSupportedType.TryGetValue(type, out var supported)) return supported;
        supported = true;
        if (type.IsGenericParameter)
        {
            logger.LogDebug("Type {0} is not supported because it is Generic Parameter", type.ToString());
            supported = false;
        }
        else
        {
            var unsupportedType = GetUsedTypes(type, false).FirstOrDefault(y => !IsInteropSupported(y));
            if (unsupportedType != null)
            {
                logger.LogDebug("Type {0} is not supported because it is uses unsupported type {1}", type.ToString(), unsupportedType.ToString());
                supported = false;
            }
        }


        isInteropSupportedType[type] = supported;
        logger.LogTrace("Type {0} is supported", type.ToString());
        return supported;
    }
    
    public static bool IsInteropSupported(MethodBase methodInfo)
    {
        // The type will not be inferrable without some magic of replacing the method signature with the type it is getting created for
        if (methodInfo.IsGenericMethod && (methodInfo.DeclaringType?.IsAbstract ?? true) && methodInfo.GetParameters().All(y=> y.ParameterType.IsAbstract))
        {
            logger.LogDebug("Type {0} Method {1} is not supported because involved types would not be inferrable (as of now)", methodInfo.DeclaringType?.FullName, methodInfo.ToString());
            return false;
        }

        logger.LogTrace("Type {0} Method {1} is supported", methodInfo.DeclaringType?.FullName, methodInfo.ToString());
        return true;
    }
}