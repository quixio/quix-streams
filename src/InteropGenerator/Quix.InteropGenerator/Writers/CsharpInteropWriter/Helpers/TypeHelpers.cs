using System;

namespace Quix.InteropGenerator.Writers.CsharpInteropWriter.Helpers;

public static class TypeHelpers
{
    public static bool IsStatic(this Type type)
    {
        return type.IsAbstract && type.IsSealed;
    }
}