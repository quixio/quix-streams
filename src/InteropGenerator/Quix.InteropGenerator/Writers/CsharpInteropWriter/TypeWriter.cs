using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
using Quix.InteropGenerator.Writers.CsharpInteropWriter.Helpers;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.CsharpInteropWriter;

public class TypeWriter : BaseWriter
{
    internal readonly Type Type;
    private readonly TypeWrittenDetails typeWrittenDetails;
    private readonly List<Type> allowedTypes;
    private readonly IEnumerable<string> usings;
    private readonly Dictionary<string, string> typeAsTextLookup;

    public TypeWriter(Type type, List<string> extraUsings, TypeWrittenDetails typeWrittenDetails, List<Type> allowedTypes)
    {
        this.Type = type;
        this.typeWrittenDetails = typeWrittenDetails;
        this.allowedTypes = allowedTypes;
        typeWrittenDetails.Type = type;
        extraUsings = (extraUsings ?? new List<string>()).Union(new[]
        {
            GetPrivateNameSpace(type),
            "System.Runtime.InteropServices",
        }).ToList();
        
        var usedTypes = Utils.GetWrappableMethodInfos(type).SelectMany(y => Utils.GetUsedTypes(type, y).SelectMany(z => new[]
            {
                Utils.GetUnmanagedType(z),
                z
            })) // These are things we don't want, like non-proper type
            .Where(y=> y.FullName != null)
            .Distinct().ToList();
        this.usings = usedTypes.Select(GetPrivateNameSpace).Union(extraUsings).OrderBy(y=>y).Distinct().ToList();
        
        // Generate type to text pairs
        var namesToFind = usedTypes.Select(y => y.Name).ToList();
        var matchingTypes = usedTypes.Select(y => y.Assembly).Distinct().SelectMany(y => y.GetTypes()).Where(y => namesToFind.Contains(y.Name)).ToList();

        var possiblyReferencedTypes = usedTypes.Select(y => y).Union(matchingTypes).Distinct().ToList();
        
        var typeNames = possiblyReferencedTypes.GroupBy(y => y.Name, y => y);
        this.typeAsTextLookup = new Dictionary<string, string>();
        foreach (var typeNameGroup in typeNames)
        {
            var nsGroups = typeNameGroup.GroupBy(y => y.Namespace).ToList();
            if (nsGroups.Count() == 1)
            {
                this.typeAsTextLookup[typeNameGroup.First().FullName] = this.GetTypeAsText(typeNameGroup.First(), false);
                continue;
            }

            foreach (var grouping in nsGroups)
            {
                foreach (var nsTypeNameGroup in grouping)
                {
                    this.typeAsTextLookup[nsTypeNameGroup.FullName] = this.GetTypeAsText(nsTypeNameGroup, true);
                }
            }
        }

    }

    public override async Task WriteContent(Func<string, Task> writeLineAction)
    {
        var indentedWriter = new IndentContentWriter(writeLineAction, 0);
        await WriteDisclaimer(indentedWriter.Write);
        await WriteUsings(indentedWriter.Write);
        await indentedWriter.WriteEmptyLine();

        await indentedWriter.Write("namespace " + AssemblyHelpers.GetInteropNamespace(this.Type) + ";");
        await indentedWriter.WriteEmptyLine();
        
        await indentedWriter.Write("public class " + Utils.GetTypeNameForNaming(this.Type) + "Interop");
        await indentedWriter.Write("{");        
        {
            indentedWriter.IncrementIndent();
            
            var nameCounter = new Dictionary<string, int>();
            var methods = Utils.GetWrappableMethodInfos(this.Type);
            var methodFields = new Dictionary<MethodInfo, FieldInfo>();

            // Create wrapping methods for fields
            foreach (var fieldInfo in this.Type.GetFields().Where(y => y.IsPublic))
            {
                var allowedToWrite = this.CheckIfAllowedToWriteField(fieldInfo);
                if (!allowedToWrite)
                {
                    Console.WriteLine($"Skipping field {this.Type.Name}.{fieldInfo.FieldType}, because contains type we won't publish");
                    continue;
                }

                var (getMethod, setMethod) = CreateFieldMethodInfos(fieldInfo);
                methods.Add(getMethod);
                methodFields[getMethod] = fieldInfo;
                
                if (setMethod == null) continue;
                methods.Add(setMethod);
                methodFields[setMethod] = fieldInfo;
            }
            
            // Write the type getting method
            //await WriteTypeMethod(indentedWriter); // Disable for now as not doing generics atm

            foreach (var typeMethodBase in methods)
            {
                var suffix = string.Empty;
                if (!nameCounter.TryGetValue(typeMethodBase.Name, out var counter))
                {
                    nameCounter[typeMethodBase.Name] = 1;
                }
                else
                {
                    nameCounter[typeMethodBase.Name] = counter + 1;
                    suffix = (counter + 1).ToString();
                }

                var disallowedTypes = this.CheckIfAllowedToWriteMethod(typeMethodBase);
                if (disallowedTypes.Count != 0)
                {
                    Console.WriteLine($"Skipping method {this.Type.Name}.{typeMethodBase.Name}{suffix}, because contains type we won't publish");
                    foreach (var type in disallowedTypes)
                    {
                        Console.WriteLine($"\t{type.FullName}");   
                    }
                    continue;
                }
                
                BaseWriter writer = null;
                var methodWrittenDetails = new MethodWrittenDetails();
                methodWrittenDetails.GetDotNetTypeAsText = this.GetTypeAsText;
                switch (typeMethodBase)
                {
                    case MethodInfo mi:
                        methodFields.TryGetValue(mi, out var fieldInfo); 
                        writer = new MethodWriter(this, mi, suffix, methodWrittenDetails, fieldInfo);
                        break;
                    case ConstructorInfo ci:
                        writer = new MethodWriter(this, ci, suffix, methodWrittenDetails);
                        break;                    
                    default:
                        throw new NotImplementedException();
                }

                try
                {
                    await writer.WriteContent(indentedWriter.Write);
                    this.typeWrittenDetails.MethodWrittenDetails.Add(methodWrittenDetails);
                }
                catch (NotSupportedMethodException e)
                {
                    Console.WriteLine(e.EntryPoint + " could not be exported");
                }
            }
            indentedWriter.DecrementIndent();
        }

        await indentedWriter.Write("}");        
    }

    public string GetInteropTypeString(Type interopType, bool isForReturn = false)
    {
        if (Utils.IsDelegate(interopType))
        {
            if (isForReturn) return "IntPtr";
            var sb = new StringBuilder();
            sb.Append("delegate* unmanaged<");
            var mi = Utils.GetMulticastDelegateMethodInfo(interopType);
            foreach (var parameterInfo in mi.GetParameters())
            {
                sb.Append(GetInteropTypeString(parameterInfo.ParameterType));
                sb.Append(", ");
            }

            sb.Append(GetInteropTypeString(mi.ReturnType));
            sb.Append(">");
            return sb.ToString();
        }
        
        var nullableType = Nullable.GetUnderlyingType(interopType);
        if (nullableType != null)
        {
            var underlyingType = Utils.GetUnmanagedType(nullableType);
            var asText = this.LookupTypeAsText(underlyingType);
            if (underlyingType == nullableType) return asText + "?";
            return asText;
        }
        
        return this.LookupTypeAsText(Utils.GetUnmanagedType(interopType));
    }

    private (MethodInfo Get, MethodInfo Set) CreateFieldMethodInfos(FieldInfo fieldInfo)
    {
        // This is an ugly workaround for me to not have to create a field writer... But it works, so hey!
        var isReadOnly = fieldInfo.IsInitOnly || fieldInfo.IsLiteral;
        MethodInfo getMethod = null, setMethod = null;
        
        var ab = AssemblyBuilder.DefineDynamicAssembly(this.Type.Assembly.GetName(), AssemblyBuilderAccess.RunAndCollect);
        var mb = ab.DefineDynamicModule(this.Type.Module.Name);
        TypeBuilder tb;
        if (this.Type.IsNested)
        {
            var nester = mb.DefineType(this.Type.DeclaringType.Name);
            tb = nester.DefineNestedType(this.Type.Name);
            nester.CreateType();
        } else tb = mb.DefineType(this.Type.Name);
            
        MethodAttributes methodAttributes = MethodAttributes.Public;
        if (fieldInfo.IsStatic) methodAttributes |= MethodAttributes.Static;
        CallingConventions callingConventions = fieldInfo.IsStatic ? CallingConventions.Standard : CallingConventions.HasThis; // TODO What was this supposed to be
        var getMethodBuilder = tb.DefineMethod($"get_{fieldInfo.Name}", methodAttributes, fieldInfo.FieldType, null);
        var ilg = getMethodBuilder.GetILGenerator();
        ilg.Emit(OpCodes.Ret);
        if (!isReadOnly)
        {
            var setMethodBuilder = tb.DefineMethod($"set_{fieldInfo.Name}", methodAttributes, typeof(void), new Type[] { fieldInfo.FieldType });
            setMethodBuilder.DefineParameter(1, ParameterAttributes.None, "value");
            ilg = setMethodBuilder.GetILGenerator();
            ilg.Emit(OpCodes.Ret);
        }

        var type = tb.CreateType();

        getMethod = type.GetMethod($"get_{fieldInfo.Name}");
        if (!isReadOnly) setMethod = type.GetMethod($"set_{fieldInfo.Name}");
        
        return (getMethod, setMethod);
    }

    private List<Type> CheckIfAllowedToWriteMethod(MethodBase methodBase)
    {
        var typesInUse = Utils.GetUsedTypes(this.Type, methodBase).SelectMany(Utils.GetUnderlyingTypes).Where(y =>
        {
            return !y.IsArray && !Utils.IsDelegate(y); // at this point these delegates are just the generic placeholders 
        }).ToList();
        var disallowedTypes = typesInUse.Where(y => !this.allowedTypes.Contains(y)).ToList(); // could be a simple all or any, but useful for debugging
        var disallowedTypesReFiltered = disallowedTypes.Where(y => this.allowedTypes.All(z => z.AssemblyQualifiedName != y.AssemblyQualifiedName)).ToList(); // this check is separate to see where it makes difference
        return disallowedTypesReFiltered;
    }
    
    private bool CheckIfAllowedToWriteField(FieldInfo fieldInfo)
    {
        var typesInUse = Utils.GetUnderlyingTypes(fieldInfo.FieldType).Where(y =>
        {
            return !y.IsArray &&
                   !Utils.IsDelegate(y); // at this point these delegates are just the generic placeholders 
        }).ToList();
        var disallowedTypes = typesInUse.Where(y => !this.allowedTypes.Contains(y)).ToList(); // could be a simple all or any, but useful for debugging
        return disallowedTypes.Count == 0;
    }

    internal string LookupTypeAsText(Type type)
    {
        if (this.typeAsTextLookup.TryGetValue(type.FullName, out var val)) return val;
        val = GetTypeAsText(type, false);
        if (this.typeAsTextLookup.Any(y=> y.Value == val && y.Key != type.FullName)) val = GetTypeAsText(type, true); // some concurrency possibility
        this.typeAsTextLookup[type.FullName] = val;
        return val;
    }

    private string GetTypeAsText(Type type, bool fullyQualified)
    {
        var replacementType = Utils.GetReplacementType(type);
        var typeText = replacementType.Name;
        if (replacementType.IsNested)
        {
            typeText = GetTypeAsText(replacementType.DeclaringType, false) + "." + replacementType.Name;
            return typeText;
        }
        if (replacementType.IsGenericType)
        {
            var nullableType = Nullable.GetUnderlyingType(replacementType);
            if (nullableType != null) return GetTypeAsText(nullableType, fullyQualified) + "?";
            var sb = new StringBuilder();
            sb.Append(replacementType.Name.Split('`')[0]);
            sb.Append("<");
            var first = true;
            foreach (var genericTypeArgument in replacementType.GenericTypeArguments)
            {
                if (first) first = false;
                else sb.Append(", ");
                sb.Append(GetTypeAsText(genericTypeArgument, fullyQualified));
            }
            sb.Append(">");
            typeText = sb.ToString();
        }
        else if (replacementType.IsArray)
        {
            return LookupTypeAsText(replacementType.GetElementType()) + "[]";
        }
        else
        {
            // This is not necessary, but prefer lowercase name for some of the types
            var synonymLookup = new Dictionary<Type, string>()
            {
                { typeof(long), "long" },
                { typeof(ulong), "ulong" },
                { typeof(int), "int" },
                { typeof(uint), "uint" },
                { typeof(string), "string" },
                { typeof(byte), "byte" },
                { typeof(sbyte), "sbyte" },
                { typeof(double), "double" },
                { typeof(float), "float" },
                { typeof(void), "void" }
            };
            if (synonymLookup.TryGetValue(replacementType, out var value)) typeText = value;
            else if (fullyQualified) typeText = replacementType.FullName;
        }

        return typeText;
    }

    private static string GetPrivateNameSpace(Type type)
    {
        return type.Namespace;
    }
    
    private async Task WriteDisclaimer(Func<string, Task> writeLineAction)
    {
        await writeLineAction("/***********************GENERATED CODE WARNING**********************");
        await writeLineAction("This file is code generated, any modification you do will be lost the");
        await writeLineAction("next time this file is regenerated.");
        await writeLineAction("********************************************************************/");
        await writeLineAction("");
    }


    private async Task WriteUsings(Func<string, Task> writeLineAction)
    {
        foreach (var typeNamespace in this.usings)
        {
            await writeLineAction($"using {typeNamespace};");
        }
    }
}