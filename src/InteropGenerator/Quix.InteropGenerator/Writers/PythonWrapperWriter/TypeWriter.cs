using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Quix.InteropGenerator.Writers.CsharpInteropWriter.Helpers;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.PythonWrapperWriter;

public class TypeWriter : BaseWriter
{
    private readonly TypeWrittenDetails typeWrittenDetails;
    private readonly Dictionary<Type, string> typeLookups;
    private readonly Dictionary<string, string> typeAsTextLookup = new Dictionary<string, string>();
    internal readonly string TypePythonName;
    internal const string GetUnderlyingReferenceFunctionName = "get_interop_ptr__"; // Unique enough hopefully
    internal const string ManualDisposeFunctionName = "dispose_ptr__"; // Unique enough hopefully

    public TypeWriter(TypeWrittenDetails typeWrittenDetails, Dictionary<Type, string> typeLookups)
    {
        this.typeWrittenDetails = typeWrittenDetails;
        this.TypePythonName = GetPythonClassName(typeWrittenDetails.Type);
        this.typeLookups = typeLookups;
    }

    private static string GetPythonClassName(Type type)
    {
        // A+B (B defined within A) will be named B only, but the file's name will reflect relationship to A to make it 
        // unique enough
        return type.FullName.Split('.')[^1].Split('+')[^1];
    }

    public override async Task WriteContent(Func<string, Task> writeLineAction)
    {
        var indentedWriter = new IndentContentWriter(writeLineAction, 0);
        await WriteDisclaimer(indentedWriter.Write);
        var extraUsings = new HashSet<Type>();

        var delayedWriter = new DelayedWriter(0);
        delayedWriter.WriteEmptyLine();
        
        delayedWriter.Write("class " + TypePythonName + "(object):");
        {
            delayedWriter.IncrementIndent();

            await WriteEnvelope(delayedWriter);
            
            // Write the type getting method
            // WriteTypeMethod(delayedWriter); // Disabled for now until future work with generics
            
            // Write all the other methods
            foreach (var typeMethodBase in this.typeWrittenDetails.MethodWrittenDetails)
            {
                BaseWriter writer = new MethodInfoWriter(this, typeMethodBase, extraUsings);
                await writer.WriteContent(delayedWriter.WriteAsync);
            }
            delayedWriter.DecrementIndent();
        }
        
        await WriteUsings(indentedWriter.Write, extraUsings);

        await indentedWriter.WriteEmptyLine();
        await delayedWriter.Flush(indentedWriter.Write);
    }

    private async Task WriteEnvelope(DelayedWriter writer)
    {
        // If the class is static, then do not write instance methods
        if (this.typeWrittenDetails.Type.IsStatic()) return;
        var blittable = Utils.IsBlittableType(this.typeWrittenDetails.Type);

        if (!blittable)
        {
            writer.WriteEmptyLine();
            await writer.WriteAsync("_weakrefs = {}");
        }

        if (!blittable) {
            // __new__
            writer.WriteEmptyLine();
            await writer.WriteAsync("def __new__(cls, net_pointer: c_void_p):");
            writer.IncrementIndent();
            await writer.WriteAsync("\"\"\"");
            await writer.WriteAsync("Parameters");
            await writer.WriteAsync("----------");
            writer.WriteEmptyLine();
            await writer.WriteAsync("net_pointer: c_void_p");
            writer.IncrementIndent();
            await writer.WriteAsync($"GC Handle Pointer to .Net type {this.typeWrittenDetails.Type.Name}");
            writer.DecrementIndent();
            writer.WriteEmptyLine();
            await writer.WriteAsync("Returns");
            await writer.WriteAsync("----------");
            writer.WriteEmptyLine();
            await writer.WriteAsync(TypePythonName +":");
            writer.IncrementIndent();
            await writer.WriteAsync($"Instance wrapping the .net type " + this.typeWrittenDetails.Type.Name);
            writer.DecrementIndent();
            await writer.WriteAsync("\"\"\"");
            await writer.WriteAsync($"if type(net_pointer) is not c_void_p:");
            writer.IncrementIndent();
            await writer.WriteAsync($"net_pointer = net_pointer.{GetUnderlyingReferenceFunctionName}()"); // if this fails, it is invalid anyway
            writer.DecrementIndent();
            writer.WriteEmptyLine();
            await writer.WriteAsync($"instance = {this.TypePythonName}._weakrefs.get(net_pointer.value)");
            await writer.WriteAsync($"if instance is None:");
            writer.IncrementIndent();
            await writer.WriteAsync($"instance = super({this.TypePythonName}, cls).__new__(cls)"); // will be initialized afterwards
            await writer.WriteAsync($"{this.TypePythonName}._weakrefs[net_pointer.value] = instance");
            writer.DecrementIndent();
            writer.WriteEmptyLine();
            await writer.WriteAsync("return instance");
            writer.DecrementIndent();
        }
        
        // __init__
        writer.WriteEmptyLine();
        await writer.WriteAsync("def __init__(self, net_pointer: c_void_p, finalize: bool = True):");
        writer.IncrementIndent();
        await writer.WriteAsync("\"\"\"");
        await writer.WriteAsync("Parameters");
        await writer.WriteAsync("----------");
        writer.WriteEmptyLine();
        await writer.WriteAsync("net_pointer: c_void_p");
        writer.IncrementIndent();
        if (!blittable) await writer.WriteAsync($"GC Handle Pointer to .Net type {this.typeWrittenDetails.Type.Name}");
        else await writer.WriteAsync($"Pointer to .Net type {this.typeWrittenDetails.Type.Name} in memory as bytes");
        writer.DecrementIndent();
        writer.WriteEmptyLine();
        await writer.WriteAsync("Returns");
        await writer.WriteAsync("----------");
        writer.WriteEmptyLine();
        await writer.WriteAsync(TypePythonName +":");
        writer.IncrementIndent();
        await writer.WriteAsync($"Instance wrapping the .net type " + this.typeWrittenDetails.Type.Name);
        writer.DecrementIndent();
        await writer.WriteAsync("\"\"\"");
        if (!blittable) { 
            await writer.WriteAsync($"if '_{TypePythonName}_pointer' in dir(self):"); // in this case we already invoked __init__
            writer.IncrementIndent();
            await writer.WriteAsync($"return");
            writer.DecrementIndent();
        }
        writer.WriteEmptyLine();
        if (!blittable)
        {
            await writer.WriteAsync($"if type(net_pointer) is not c_void_p:");
            writer.IncrementIndent();
            await writer.WriteAsync($"self._pointer_owner = net_pointer");
            await writer.WriteAsync($"self._pointer = net_pointer.{GetUnderlyingReferenceFunctionName}()"); // this should not fail at this point, __new__ would fail before
            writer.DecrementIndent();
            await writer.WriteAsync($"else:");
            writer.IncrementIndent();
            await writer.WriteAsync($"self._pointer = net_pointer");
            writer.DecrementIndent();
        }
        else await writer.WriteAsync($"self._pointer = net_pointer");
        writer.WriteEmptyLine();
        await writer.WriteAsync("if finalize:");
        writer.IncrementIndent();
        await writer.WriteAsync("self._finalizer = weakref.finalize(self, self._finalizerfunc)"); // https://docs.python.org/3/library/weakref.html
        await writer.WriteAsync("self._finalizer.atexit = False"); // done to avoid cleaning resources at exit to easier track leaks
        writer.DecrementIndent();
        await writer.WriteAsync("else:");
        writer.IncrementIndent();
        await writer.WriteAsync("self._finalizer = lambda: None");
        writer.DecrementIndent();
        writer.DecrementIndent();
        
        // __finalizerfunc
        writer.WriteEmptyLine();
        await writer.WriteAsync("def _finalizerfunc(self):");
        writer.IncrementIndent();
        //await writer.WriteAsync("if InteropUtils.DebugEnabled:");
        //writer.IncrementIndent();
        //await writer.WriteAsync($"print(\"Python is finalizing {this.TypePythonName} with addr {{}}\".format(self._pointer))");
        //writer.DecrementIndent();
        if (!blittable) await writer.WriteAsync($"del {this.TypePythonName}._weakrefs[self._pointer.value]");
        if (!blittable) await writer.WriteAsync("InteropUtils.free_hptr(self._pointer)");
        else await writer.WriteAsync("InteropUtils.free_uptr(self._pointer)");
        await writer.WriteAsync("self._finalizer.detach()");
        writer.DecrementIndent();
        
        // GetUnderlyingReferenceFunctionName
        writer.WriteEmptyLine();
        await writer.WriteAsync($"def {GetUnderlyingReferenceFunctionName}(self) -> c_void_p:");
        writer.IncrementIndent();
        await writer.WriteAsync($"return self._pointer");
        writer.DecrementIndent();
        
        // ManualDisposeFunctionName
        writer.WriteEmptyLine();
        await writer.WriteAsync($"def {ManualDisposeFunctionName}(self):");
        writer.IncrementIndent();
        await writer.WriteAsync($"self._finalizer()");
        writer.DecrementIndent();
        
        // __enter__
        writer.WriteEmptyLine();
        await writer.WriteAsync($"def __enter__(self):");
        writer.IncrementIndent();
        await writer.WriteAsync($"pass");
        writer.DecrementIndent();
        
        // __exit__
        writer.WriteEmptyLine();
        await writer.WriteAsync($"def __exit__(self, exc_type, exc_val, exc_tb):");
        writer.IncrementIndent();
        await writer.WriteAsync($"self._finalizer()");
        writer.DecrementIndent();
    } 

    public string GetPythonTypeString(Type interopType)
    {
        if (Utils.IsDelegate(interopType))
        {
            var sb = new StringBuilder();
            sb.Append("Callable[[");
            if (interopType == typeof(Delegate))
            {
                // dummy delegate
                sb.Append("]]");
                return sb.ToString();
            }

            var mi = Utils.GetMulticastDelegateMethodInfo(interopType);
            var first = true;
            foreach (var parameterInfo in mi.GetParameters())
            {
                if (!first) sb.Append(", ");
                first = false;
                sb.Append(GetPythonTypeString(parameterInfo.ParameterType));
            }

            sb.Append("], ");

            sb.Append(GetPythonTypeString(mi.ReturnType));
            sb.Append("]");
            return sb.ToString();
        }
        
        return this.LookupTypeAsText(Utils.GetPythonMappableType(interopType));
    }

    public string LookupTypeAsText(Type type)
    {
        if (this.typeAsTextLookup.TryGetValue(type.FullName, out var val)) return val;
        val = GetTypeAsText(type, false);
        if (this.typeAsTextLookup.Any(y => y.Value == val && y.Key != type.FullName)) val = GetTypeAsText(type, true); // some concurrency possibility
        this.typeAsTextLookup[type.FullName] = val;
        return val;
    }
    
    private string GetTypeAsText(Type type, bool fullyQualified)
    {
        var typeText = type.Name;
        if (type.IsNested)
        {
            typeText = type.Name;
        }

        if (type.IsGenericType)
        {
            var nullableType = Nullable.GetUnderlyingType(type);
            if (nullableType != null) return "Optional[" + GetTypeAsText(Nullable.GetUnderlyingType(type), fullyQualified) +"]";
            var sb = new StringBuilder();
            sb.Append(type.Name.Split('`')[0]);
            sb.Append("<");
            var first = true;
            foreach (var genericTypeArgument in type.GenericTypeArguments)
            {
                if (first) first = false;
                else sb.Append(", ");
                sb.Append(GetTypeAsText(genericTypeArgument, fullyQualified));
            }
            sb.Append(">");
            typeText = sb.ToString();
        }
        else
        {
            // This is not necessary, but prefer lowercase name for some of the types
            var synonymLookup = new Dictionary<Type, string>()
            {
                { typeof(long), "int" },
                { typeof(ulong), "int" },
                { typeof(int), "int" },
                { typeof(uint), "int" },
                { typeof(string), "str" },
                { typeof(byte), "int" },
                { typeof(sbyte), "int" },
                { typeof(double), "float" },
                { typeof(float), "float" },
                { typeof(void), "None" },
                { typeof(IntPtr), "c_void_p" },
                { typeof(bool), "bool" }
            };
            if (synonymLookup.TryGetValue(type, out var value)) typeText = value;
            else if (fullyQualified) typeText = type.FullName;
        }

        return typeText.Split('+')[^1]; // types defined in other types
    }

    private static string GetPrivateNameSpace(Type type)
    {
        return type.Namespace;
    }
    
    private void WriteTypeMethod(DelayedWriter writer)
    {
        writer.WriteEmptyLine();
        writer.Write("# ctypes function return type//parameter fix");
        writer.Write($"interop_func = InteropUtils.get_function(\"{this.typeWrittenDetails.TypeMethodEntryPoint}\")");
        writer.Write("interop_func.restype = c_void_p");
        writer.Write("Type = interop_func()");
    }
    
    private async Task WriteDisclaimer(Func<string, Task> writeLineAction)
    {
        await writeLineAction("# ***********************GENERATED CODE WARNING************************");
        await writeLineAction("# This file is code generated, any modification you do will be lost the");
        await writeLineAction("# next time this file is regenerated.");
        await writeLineAction("# *********************************************************************");
        await writeLineAction("");
    }


    private async Task WriteUsings(Func<string, Task> writeLineAction, HashSet<Type> extraUsings)
    {
        extraUsings.Add(ExtraTypes.GetType("InteropUtils"));
        await writeLineAction($"import ctypes");
        await writeLineAction($"import weakref");
        await writeLineAction($"from typing import Optional");
        await writeLineAction($"from ctypes import c_void_p");
        var usings = CreateUsings(extraUsings);
        foreach (var @using in usings)
        {
            var submodules = @using.Value;
            if (submodules.Count == 0)
            {
                await writeLineAction("import " + @using.Key);
                continue;
            }

            var sb = new StringBuilder();
            sb.Append("from ");
            sb.Append(@using.Key);
            sb.Append(" import ");
            sb.AppendJoin(", ", submodules);
            await writeLineAction(sb.ToString());
        }
    }

    private Dictionary<string, HashSet<string>> CreateUsings(HashSet<Type> types)
    {
        var dictionary = new Dictionary<string, HashSet<string>>();
        if (!this.typeLookups.TryGetValue(this.typeWrittenDetails.Type, out var currentModule))
        {
            throw new Exception("Something is wrong");
        }

        foreach (var type in types)
        {
            if (type == this.typeWrittenDetails.Type) continue; // Not necessary to import itself
            var module = string.Empty;
            var subModule = string.Empty;
            // check if the type is a type we're creating
            if (!this.typeLookups.TryGetValue(type, out var importModule))
            {
                // If the quick dictionary lookup is not successful, check if we have the type but for some reason not the exact same TODO why is this happening
                importModule = this.typeLookups.FirstOrDefault(y => y.Key.AssemblyQualifiedName == type.AssemblyQualifiedName).Value;
            }
            if (importModule != null)
            {
                var relativePath = Path.GetRelativePath(currentModule, importModule);
                if (relativePath.StartsWith("..\\") || relativePath.StartsWith("../"))
                {
                    relativePath = relativePath.Substring(3);
                }
                module = Regex.Replace(relativePath, "\\.py$", "");;
                module = module.Replace("../", ".").Replace("..\\", ".").TrimEnd('\\', '/');
                module = "." + module;
                
                subModule = GetPythonClassName(type);
            }

            // check if the type is a known handled type
            if (module == string.Empty && typeof(Delegate).IsAssignableFrom(type))
            {
                module = "typing";
                subModule = "Callable";
            }

            // we don't know how to handle rest, may be built in type or such

            if (module == string.Empty) continue;
            module = module.Replace("\\",".").Replace("/", ".");
            if (!dictionary.TryGetValue(module, out var subModules))
            {
                subModules = new HashSet<string>();
                dictionary[module] = subModules;
            }

            if (subModule == string.Empty) continue; 
            
            subModules.Add(subModule);
        }

        return dictionary;
    }
}