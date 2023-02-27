using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace Quix.InteropGenerator.Writers.PythonWrapperWriter;

public class ExtraTypes
{
    private static List<Type> extraTypes = new List<Type>();

    static ExtraTypes()
    {
        AssemblyName aName = new AssemblyName("Extra");
        AssemblyBuilder ab =
            AssemblyBuilder.DefineDynamicAssembly(
                aName,
                AssemblyBuilderAccess.Run);

        // The module name is usually the same as the assembly name.
        ModuleBuilder mb =
            ab.DefineDynamicModule(aName.Name);

        TypeBuilder interoputils = mb.DefineType(
            "InteropUtils",
            TypeAttributes.Public);

        extraTypes.Add(interoputils.CreateType());
    }

    public  static Type GetType(string typeName)
    {
        return extraTypes.FirstOrDefault(y => y.Name == typeName);
    }
}