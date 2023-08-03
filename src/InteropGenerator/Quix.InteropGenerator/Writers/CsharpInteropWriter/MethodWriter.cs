using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using InteropHelpers.Interop;
using InteropHelpers.Interop.ExternalTypes.System;
using Microsoft.Extensions.Primitives;
using Quix.InteropGenerator.Writers.CsharpInteropWriter.Helpers;
using Quix.InteropGenerator.Writers.Shared;

namespace Quix.InteropGenerator.Writers.CsharpInteropWriter;

/// <summary>
/// Method (not MethodInfo) writer, as it can use more than just MethodInfo
/// </summary>
public class MethodWriter : BaseWriter
{
    private readonly TypeWriter typeWriter;
    private readonly MethodBase methodBase;
    private readonly ParameterInfo[] parameterInfos;
    private Type returnType;
    private readonly string nameSuffix;
    private readonly MethodWrittenDetails methodWrittenDetails;
    private readonly FieldInfo fieldInfo;
    private readonly ParameterInfo[] methodParameterOrder;

    /// <summary>
    /// 
    /// </summary>
    /// <param name="typeWriter"></param>
    /// <param name="methodBase"></param>
    /// <param name="nameSuffix"></param>
    /// <param name="methodWrittenDetails"></param>
    /// <param name="fieldInfo">Optional field info where the method is actually a field</param>
    public MethodWriter(TypeWriter typeWriter, MethodBase methodBase, string nameSuffix, MethodWrittenDetails methodWrittenDetails, FieldInfo fieldInfo = null)
    {
        this.typeWriter = typeWriter;
        this.methodBase = methodBase;
        this.parameterInfos = methodBase.GetParameters();
        this.methodParameterOrder = this.parameterInfos.OrderBy(y=> y.IsOptional).ToArray(); // always have non-optional first;
        this.returnType = methodBase is ConstructorInfo ci 
            ? ci.DeclaringType
            : methodBase is MethodInfo mi 
                ? mi.ReturnType
                : typeof(void);
        this.nameSuffix = nameSuffix;
        this.methodWrittenDetails = methodWrittenDetails;
        this.fieldInfo = fieldInfo;
        this.methodWrittenDetails.DeclaringType = this.typeWriter.Type;
        this.methodWrittenDetails.UniqueMethodName = this.GetMethodName();
        this.methodWrittenDetails.EntryPoint = GetEntryPointName();        
        this.methodWrittenDetails.MethodBase = this.methodBase;
        this.methodWrittenDetails.ReturnType = this.returnType;
        this.methodWrittenDetails.ParameterInfos = this.parameterInfos;
        this.methodWrittenDetails.MethodParameterOrder = this.methodParameterOrder;
    }

    public override async Task WriteContent(Func<string, Task> writeLineAction)
    {
        var preFunctionWriter = new DelayedWriter(0);
        var bodyWriter = new DelayedWriter(0);
        var throwAwayWriter = new DelayedWriter(0);
        // Get the converted parameter args
        var args = WriteFunctionHeader(throwAwayWriter);
        WriteFunctionBody(bodyWriter, args, preFunctionWriter);

        if (!preFunctionWriter.IsEmpty)
        {
            await writeLineAction(""); // empty line
            await preFunctionWriter.Flush(writeLineAction);
        }
        
        var headerWriter = new DelayedWriter(0);
        headerWriter.WriteEmptyLine(); // separate from other things

        WriteFunctionHeader(headerWriter);

        await headerWriter.Flush(writeLineAction);
        await bodyWriter.Flush(writeLineAction);
    }
    
    private void WriteFunctionBody(DelayedWriter writer, Dictionary<string, string> argNames, DelayedWriter preFunctionWriter)
    {
        using var iw = new IndentWriter<DelayedWriter>(writer);
        writer.Write($"InteropUtils.LogDebug($\"Invoking entrypoint {this.methodWrittenDetails.EntryPoint}\");");
        writer.Write($"InteropUtils.LogDebugIndentIncr();");
        using var ehw = new ExceptionHandlerWriter<DelayedWriter>(writer, () => this.returnType, delayedWriter =>
        {
            delayedWriter.Write($"InteropUtils.LogDebug(\"Exception in {this.methodWrittenDetails.EntryPoint}\");");
            foreach (var argName in argNames)
            {
                if (argName.Value.Contains("delegate*"))
                {
                    delayedWriter.Write($"InteropUtils.LogDebug($\"Arg {argName.Key} ({argName.Value}) has value {{(IntPtr){argName.Key}}}\");");
                }
                else delayedWriter.Write($"InteropUtils.LogDebug($\"Arg {argName.Key} ({argName.Value}) has value: {{{argName.Key}}}\");");   
            }
        }, (w) =>
        {
            writer.Write($"InteropUtils.LogDebugIndentDecr();");
            writer.Write($"InteropUtils.LogDebug($\"Invoked entrypoint {this.methodWrittenDetails.EntryPoint}\");");    
        });
        
        
        if (this.methodBase.IsSpecialName)
        {
            HandleSpecialName(writer, preFunctionWriter);
        }
        else
        {
            HandleRegularMethod(writer, preFunctionWriter);
        }
    }

    private void HandleSpecialName(DelayedWriter writer, DelayedWriter preFunctionWriter)
    {
        var name = methodBase.Name;
        if (name.StartsWith("add_"))
        {
            HandleAddMethod(writer);
        }
        else if (name.StartsWith("remove_"))
        {
            HandleRemoveMethod(writer);
        }
        else if (name.StartsWith("get_"))
        {
            HandlePropertyGetter(writer, preFunctionWriter);
        }
        else if (name.StartsWith("set_"))
        {
            HandlePropertySetter(writer);
        }
        else if (name.StartsWith("op_Equality"))
        {
            HandleOperator(writer, preFunctionWriter, "==");
        }
        else if (name.StartsWith("op_Inequality"))
        {
            HandleOperator(writer, preFunctionWriter, "!=");
        }
        else if (this.methodBase.IsConstructor)
        {
            HandleConstructor(writer, preFunctionWriter);
        }
        else
        {
            throw new NotSupportedMethodException(this.methodWrittenDetails.EntryPoint);
        }
    }

    private void WriteArgumentConversion(DelayedWriter writer, DelayedWriter preFunctionWriter, out Dictionary<string, string> nameConversions, out Dictionary<string, Type> paramType, out string instanceArgName)
    {
        nameConversions = new Dictionary<string, string>();
        paramType = new Dictionary<string, Type>();
        var anyRecast = false;

        instanceArgName = Utils.GetInstanceParameterName(this.methodWrittenDetails.DeclaringType, this.methodBase);
        if (instanceArgName != null)
        {
            paramType[instanceArgName] = this.methodBase.DeclaringType;
            var result = WriteTypeConversion(writer, preFunctionWriter, this.methodBase.DeclaringType, false,
                instanceArgName);
            nameConversions[instanceArgName] = result.Name;
            paramType[result.Name] = result.Type;
            anyRecast |= instanceArgName != result.Name;
        }

        foreach (var parameterInfo in this.parameterInfos)
        {
            paramType[parameterInfo.Name] = parameterInfo.ParameterType;

            if (parameterInfo.IsOut)
            {
                nameConversions[parameterInfo.Name] = parameterInfo.Name + "Out";
                continue;
            }

            var result = WriteTypeConversion(writer, preFunctionWriter, parameterInfo.ParameterType, false,
                parameterInfo.Name);
            nameConversions[parameterInfo.Name] = result.Name;
            paramType[result.Name] = result.Type;
            anyRecast |= instanceArgName != result.Name;

            nameConversions[parameterInfo.Name] = WriteDefaultValue(writer, parameterInfo, nameConversions[parameterInfo.Name]);
        }

        if (anyRecast) writer.WriteEmptyLine();
    }

    private void HandleRegularMethod(DelayedWriter writer, DelayedWriter preFunctionWriter)
    {
        WriteArgumentConversion(writer, preFunctionWriter, out var paramNames, out var paramTypes, out var instanceArgName);

        var sb = new StringBuilder();
        var resultName = string.Empty;
        if (this.returnType != typeof(void))
        {
            resultName = GetResultName(paramNames);

            sb.Append("var ");
            sb.Append(resultName);
            sb.Append(" = ");
        }

        var lineFinish = ";";
        var isExtensionMethod = this.methodBase.IsDefined(typeof(ExtensionAttribute), true);
        var customInvocation = false;
        if (!isExtensionMethod)
        {
            if (this.methodBase.IsStatic)
            {
                sb.Append(this.methodBase.DeclaringType.Name);
                sb.Append(".");
            }
            else if (instanceArgName != null)
            {
                sb.Append(paramNames[instanceArgName]);
                sb.Append(".");
            }

            if (this.fieldInfo == null)
            {
                sb.Append(this.methodBase.Name);
                lineFinish = ");";
                sb.Append("(");
            }
            else
            {
                sb.Append(this.fieldInfo.Name);
                if (methodBase.Name.StartsWith("set_")) sb.Append(" = ");
            }
        }

        if (!customInvocation)
        {
            WriteManagedMethodInvocation(sb, paramNames);
        }
        
        sb.Append(lineFinish);

        writer.Write(sb.ToString());

        foreach (var parameterInfo in this.parameterInfos)
        {
            if (parameterInfo.IsOut)
            {
                var result = WriteTypeConversion(writer, preFunctionWriter, parameterInfo.ParameterType, true,
                    paramNames[parameterInfo.Name]);
                writer.Write($"Marshal.WriteIntPtr({parameterInfo.Name}, {result.Name});");
            }
        }


        if (!string.IsNullOrWhiteSpace(resultName))
        {
            var result = WriteTypeConversion(writer, preFunctionWriter, this.returnType, true, resultName);
            writer.Write($"return {result.Name};");
        }
    }

    private StringBuilder WriteManagedMethodInvocation(StringBuilder sb, Dictionary<string, string> paramNames)
    {
        var doComma = false;
        var first = true;
        var isExtensionMethod = this.methodBase.IsDefined(typeof(ExtensionAttribute), true);
        var finish = "";
        foreach (var parameterInfo in this.parameterInfos)
        {
            if (first)
            {
                first = false;
                if (isExtensionMethod)
                {
                    sb.Append(paramNames[parameterInfo.Name]);
                    sb.Append(".");
                    sb.Append(this.methodBase.Name);
                    finish = ")";
                    sb.Append("(");
                    continue;
                }
            }

            if (doComma) sb.Append(", ");
            if (parameterInfo.IsOut)
            {
                sb.Append("out var ");
            }

            sb.Append(paramNames[parameterInfo.Name]);
            doComma = true;
        }
        sb.Append(finish);
        return sb;
    }

    private void HandleAddMethod(DelayedWriter writer)
    {
        WriteArgumentConversion(writer, null, out var paramNames, out var paramTypes, out var instanceArgName);

        var sb = new StringBuilder();
        // Write the actual subscription
        if (this.methodBase.IsStatic) sb.Append(Utils.GetTypeNameForNaming(this.methodBase.DeclaringType));
        else sb.Append(paramNames[instanceArgName]);
        sb.Append(".");
        sb.Append(this.methodBase.Name.Substring("add_".Length));
        sb.Append(" += ");
        sb.Append(paramNames[this.parameterInfos.First().Name]);
        sb.Append(";");
        writer.Write(sb.ToString());
        
        
        // Write the conversion from the handler to pointer that we can use to unsubscribe with
        var del = this.parameterInfos.First();
        var convertedName = paramNames[del.Name];
        ReplaceReturnType(typeof(IntPtr));
        var resultName = convertedName + "Ptr";
        writer.Write($"var {resultName} = InteropUtils.ToHPtr({convertedName});");
        
        
        var handlerTypeAsText = typeWriter.LookupTypeAsText(paramTypes[this.parameterInfos[0].Name]);
        writer.Write($"InteropUtils.LogDebug($\"Added handler type \\\"{handlerTypeAsText}\\\" with ptr value {{{resultName}}}.\");");

        
        writer.Write($"return {resultName};");
    }

    private void HandleRemoveMethod(DelayedWriter writer)
    {
        var voidWriter = new DelayedWriter();
        WriteArgumentConversion(voidWriter, voidWriter, out var paramNames, out var paramTypes, out var instanceArgName);

        var replaced = new ReplacedParameterInfo(this.parameterInfos[0], typeof(IntPtr));
        var handlerName = GetResultName(paramNames, "handler");
        var valParam = replaced.Original;
        var handlerTypeAsText = typeWriter.LookupTypeAsText(paramTypes[valParam.Name]);
        var handlerConversionText = $"var {handlerName} = {nameof(InteropUtils)}.{nameof(InteropUtils.FromHPtr)}<{handlerTypeAsText}>({valParam.Name});";
        
        ReplaceParameterInfo(replaced);
        
        WriteArgumentConversion(writer, voidWriter, out paramNames, out paramTypes, out instanceArgName);

        writer.Write(handlerConversionText);
        
        writer.Write($"InteropUtils.LogDebug($\"Removing handler type \\\"{handlerTypeAsText}\\\" with ptr value {{{valParam.Name}}}.\");");

        
        var sb = new StringBuilder();
        // Write the actual subscription
        if (this.methodBase.IsStatic) sb.Append(Utils.GetTypeNameForNaming(this.methodBase.DeclaringType));
        else sb.Append(paramNames[instanceArgName]);
        sb.Append(".");
        sb.Append(this.methodBase.Name.Substring("remove_".Length));
        sb.Append(" -= ");
        sb.Append(handlerName);
        sb.Append(";");
        writer.Write(sb.ToString());
    }

    private void HandlePropertyGetter(DelayedWriter writer, DelayedWriter preFunctionWriter)
    {
        WriteArgumentConversion(writer, null, out var paramNames, out var paramTypes, out var instanceArgName);
        var sb = new StringBuilder();
        var resultName = GetResultName(paramNames);
        sb.Append($"var {resultName} = ");
        var methodName = this.methodBase.Name.Substring("get_".Length);
        if (this.methodBase.IsStatic) sb.Append(Utils.GetTypeNameForNaming(this.methodBase.DeclaringType));
        else sb.Append(paramNames[instanceArgName]);

        if (this.parameterInfos.Length == 0)
        {
            sb.Append(".");
            sb.Append(methodName);
            WriteManagedMethodInvocation(sb, paramNames);
            sb.Append(";");
        }
        else // indexed prop
        {
            sb.Append("[");
            sb.Append(paramNames[this.parameterInfos.First().Name]);
            sb.Append("];");
        }

        writer.Write(sb.ToString());
        
        var result = WriteTypeConversion(writer, preFunctionWriter, this.returnType, true, resultName);
        writer.Write($"return {result.Name};");
    }

    private void HandlePropertySetter(DelayedWriter writer)
    {
        WriteArgumentConversion(writer, null, out var paramNames, out var paramTypes, out var instanceArgName);
        var sb = new StringBuilder();
        var methodName = this.methodBase.Name.Substring("set_".Length);
        if (this.methodBase.IsStatic) sb.Append(Utils.GetTypeNameForNaming(this.methodBase.DeclaringType));
        else sb.Append(paramNames[instanceArgName]);

        if (this.parameterInfos.Length == 1)
        {
            sb.Append(".");
            sb.Append(methodName);
            sb.Append(" = ");
            WriteManagedMethodInvocation(sb, paramNames);
            sb.Append(";");
            writer.Write(sb.ToString());
        }
        else
        {
            sb.Append("[");
            sb.Append(paramNames[this.parameterInfos[0].Name]);
            sb.Append("] = ");
            sb.Append(paramNames[this.parameterInfos[1].Name]);
            sb.Append(";");
        }
        writer.Write(sb.ToString());
    }

    private void HandleOperator(DelayedWriter writer, DelayedWriter preFunctionWriter, string operatorText)
    {
        WriteArgumentConversion(writer, null, out var paramNames, out var paramTypes, out var instanceArgName);

        var sb = new StringBuilder();
        
        var resultName = GetResultName(paramNames, "operationResult");
        sb.Append($"var {resultName} = ");
        
        sb.Append(paramNames[this.parameterInfos[0].Name]);
        sb.Append($" {operatorText} ");
        sb.Append(paramNames[this.parameterInfos[1].Name]);
        sb.Append(";");
        writer.Write(sb.ToString());
        
        var result = WriteTypeConversion(writer, preFunctionWriter, this.returnType, true, resultName);
        writer.Write($"return {result.Name};");
    }

    private void HandleConstructor(DelayedWriter writer, DelayedWriter preFunctionWriter)
    {
        WriteArgumentConversion(writer, null, out var paramNames, out var paramTypes, out var instanceArgName);

        var sb = new StringBuilder();
        
        var resultName = GetResultName(paramNames, "constructorResult");
        sb.Append($"var {resultName} = ");
        
        sb.Append("new ");
        sb.Append(typeWriter.LookupTypeAsText(this.methodBase.DeclaringType));
        sb.Append("(");
        WriteManagedMethodInvocation(sb, paramNames);
        sb.Append(");");
        writer.Write(sb.ToString());
        
        var result = WriteTypeConversion(writer, preFunctionWriter, this.returnType, true, resultName);
        writer.Write($"return {result.Name};");
    }

    private string GetResultName(Dictionary<string,string> paramNames, string baseName = "result")
    {
        var resultName = baseName;
        var counter = 1;
        while (paramNames.Any(y => y.Key == resultName || y.Value == resultName))
        {
            counter++;
            resultName = baseName + counter;
        }

        return resultName;
    }
    
    

    private void ReplaceParameterInfo(ReplacedParameterInfo replacedParameterInfo)
    {
        for (var ii = 0; ii <= this.parameterInfos.Length; ii++)
        {
            if (this.parameterInfos[ii] != replacedParameterInfo.Original) continue;
            this.parameterInfos[ii] = replacedParameterInfo;
            break;
        }
        
        for (var ii = 0; ii <= this.methodParameterOrder.Length; ii++)
        {
            if (this.methodParameterOrder[ii] != replacedParameterInfo.Original) continue;
            this.methodParameterOrder[ii] = replacedParameterInfo;
            break;
        }
    }

    private void ReplaceReturnType(Type type)
    {
        this.returnType = type;
        this.methodWrittenDetails.ReturnType = type;
    }

    private string WriteDefaultValue(DelayedWriter writer, ParameterInfo parameterInfo, string variableName)
    {
        if (!parameterInfo.IsOptional) return variableName;
        if (!parameterInfo.HasDefaultValue) return variableName;
        if (parameterInfo.ParameterType == typeof(bool)) return variableName; // this is handled in function header properly
        if (parameterInfo.DefaultValue == null) return variableName; // there is nothing to default to
        if (parameterInfo.IsOptional && Utils.IsEnum(parameterInfo.ParameterType)) return variableName;

        var sb = new StringBuilder();

        sb.Append(variableName);
        sb.Append(" = ");
        sb.Append(variableName);
        sb.Append(" != default ? ");
        sb.Append(variableName);
        sb.Append(" : ");
        if (parameterInfo.DefaultValue.GetType() == typeof(string))
        {
            sb.Append("\"");
            sb.Append(parameterInfo.DefaultValue);
            sb.Append("\"");
        } else if (parameterInfo.DefaultValue is Enum)
        {
            sb.Append(parameterInfo.DefaultValue.GetType().Name);
            sb.Append(".");
            sb.Append(parameterInfo.DefaultValue);
        }
        else
        {
            sb.Append(parameterInfo.DefaultValue);
        }

        sb.Append(";");
        writer.Write(sb.ToString());
        return variableName;
    }

    /// <summary>
    /// Creates C# code to convert the type between managed and unmanaged
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="preFunctionWriter">writer to add something before the function that contains this conversion</param>
    /// <param name="typeToConvert"></param>
    /// <param name="toUnmanaged"></param>
    /// <param name="sourceName"></param>
    /// <param name="targetName"></param>
    /// <param name="createTargetVar"></param>
    /// <returns>The name of the variable the converted result will be assigned to</returns>
    /// <exception cref="Exception"></exception>
    /// <exception cref="NotSupportedMethodException"></exception>
    private TypeConversionResult WriteTypeConversion(DelayedWriter writer, DelayedWriter preFunctionWriter, Type typeToConvert, bool toUnmanaged, string sourceName, string targetName = null, bool createTargetVar = true)
    {
        Type source, target;
        if (toUnmanaged)
        {
            source = typeToConvert;
            target = Utils.GetUnmanagedType(typeToConvert);
        }
        else
        {
            source = Utils.GetUnmanagedType(typeToConvert);
            target = typeToConvert;
        }
        
        sourceName = GetValidParameterName(sourceName); // none of the generated ones should be a reserved keyword, and if it is a parameter of a method, then it'll be the result of this method anyway
        var targetVarCreate = (createTargetVar ? "var " : "");
        if (target == source) return new TypeConversionResult(sourceName, typeToConvert);

        if (source == typeof(IntPtr))
        {
            if (Utils.GetReplacementType(target) == typeof(string))
            {
                targetName ??= $"{sourceName}Str";
                writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.PtrToStringUTF8)}({sourceName});");
                if (target == typeof(Span<char>)) // Span isn't string equatable, we need to convert
                {
                    var readSpanTargetName = $"{targetName}ReadSpan";
                    writer.Write($"var {readSpanTargetName} = {targetName}.AsSpan();");
                    var spanTargetName = $"{targetName}Span";
                    writer.Write($"var {spanTargetName} = new Span<char>(GC.AllocateUninitializedArray<char>({readSpanTargetName}.Length));");
                    writer.Write($"{readSpanTargetName}.CopyTo({spanTargetName});");
                    targetName = spanTargetName;
                }

                return new TypeConversionResult(targetName, typeof(string));
            }
            
            if (!toUnmanaged && Utils.IsBlittableType(target))
            {
                targetName ??= $"{sourceName}UPtr";
                writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.FromUPtr)}<{target.Name}>({sourceName});");
                return new TypeConversionResult(targetName, target);
            }

            if (typeof(IDictionary).IsAssignableFrom(target))
            {
                targetName ??= $"{sourceName}Dict";
                var dictTypeText = typeWriter.LookupTypeAsText(target);
                var generics = target.GetGenericArguments();
                if (generics.Length == 0)
                {
                    var firstKeyValuePairType = target.GetInterfaces().Select(y=> y.GetGenericArguments()
                        .FirstOrDefault(x=> x.IsGenericType  && x.GetGenericTypeDefinition() == typeof(KeyValuePair<,>))).FirstOrDefault(y=> y!= null);
                    if (firstKeyValuePairType == null) throw new Exception($"Unable to look up Dictionary generic type for {target}");
                    generics = firstKeyValuePairType.GetGenericArguments();

                }
                var keyTypeText = typeWriter.LookupTypeAsText(generics[0]);
                var valueTypeText = typeWriter.LookupTypeAsText(generics[1]);
                if (Utils.HasParameterlessConstructor(target)) {
                    writer.Write($"{targetVarCreate}{targetName} = {nameof(DictionaryInterop)}.{nameof(DictionaryInterop.FromUPtr)}<{dictTypeText}, {keyTypeText}, {valueTypeText}>({sourceName});");
                }
                else
                {
                    writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.FromHPtr)}<{dictTypeText}>({sourceName});");

                }
                return new TypeConversionResult(targetName, target);
            }
            
            if (source != typeof(Array) && typeof(Array).IsAssignableFrom(target))
            {
                // The following is using array allocation with size rather than linq in order to improve performance and
                // remove the need for referencing LINQ. Resulting code is an eyesore, but better overall
                var unmanagedArrayName = $"{sourceName}ArrUnmanaged";
                var generic = target.HasElementType ? target.GetElementType() : typeof(object);
                var elementTypeAsText = typeWriter.LookupTypeAsText(generic);
                var elementTypeAsUnmanagedText = typeWriter.GetInteropTypeString(generic, false);
                writer.Write($"var {unmanagedArrayName} = {nameof(InteropUtils)}.{nameof(InteropUtils.FromArrayUPtr)}({sourceName}, typeof({elementTypeAsUnmanagedText})) as {elementTypeAsUnmanagedText}[];");
                targetName ??= $"{sourceName}Arr";
                
                // There seem to be a bug in NAOT where byte[][len] is not working but (byte[][])Array.CreateInstance(typeof(byte[]), len) does
                writer.Write($"{targetVarCreate}{targetName} = {unmanagedArrayName} == null ? null : ({elementTypeAsText}[])Array.CreateInstance(typeof({elementTypeAsText}), {unmanagedArrayName}.Length);");
                writer.Write($"if ({targetName} != null) {{"); // start of null check
                writer.IncrementIndent();
                writer.Write($"for (var {targetName}Index = 0; {targetName}Index < {targetName}.Length; {targetName}Index++) {{"); // start of for
                writer.IncrementIndent();
                var innerBodyConvertResultName = WriteTypeConversion(writer, preFunctionWriter, generic, false, $"{unmanagedArrayName}[{targetName}Index]", $"{targetName}Converted");
                writer.Write($"{targetName}[{targetName}Index] = {innerBodyConvertResultName.Name};");
                writer.DecrementIndent();
                writer.Write("}"); // end of for
                writer.DecrementIndent();
                writer.Write($"}}"); // end of null check
                return new TypeConversionResult(targetName, target);
            }
           
            targetName ??= $"{sourceName}Inst";
            
            var typeText = typeWriter.LookupTypeAsText(target);

            writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.FromHPtr)}<{typeText}>({sourceName});");
            return new TypeConversionResult(targetName, target);
        }
        
        if (target == typeof(IntPtr))
        {
            if (Utils.GetReplacementType(source) == typeof(string))
            {
                targetName ??= $"{sourceName}UPtr";
                writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.Utf8StringToUPtr)}({sourceName});");
                return new TypeConversionResult(targetName, target);
            }

            if (toUnmanaged && Utils.IsBlittableType(source))
            {
                targetName ??= $"{sourceName}UPtr";
                writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.ToUPtr)}({sourceName});");
                return new TypeConversionResult(targetName, target);
            }

            if (source != typeof(Array) && typeof(Array).IsAssignableFrom(source))
            {
                // The following is using array allocation with size rather than linq in order to improve performance and
                // remove the need for referencing LINQ. Resulting code is an eyesore, but better overall
                targetName ??= $"{sourceName}ArrUnmanaged";
                var generic = source.GetElementType();
                var elementTypeAsText = typeWriter.LookupTypeAsText(generic);
                if (generic == typeof(string))
                {
                    writer.Write($"var {targetName} = {nameof(CollectionInterop)}.{nameof(CollectionInterop.ToUPtrString)}({sourceName});");
                }
                else
                {
                    writer.Write($"var {targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.ToArrayUPtr)}({sourceName}, typeof({elementTypeAsText}));");
                }

                return new TypeConversionResult(targetName, target);
            }

            targetName ??= $"{sourceName}HPtr";
            writer.Write($"{targetVarCreate}{targetName} = {nameof(InteropUtils)}.{nameof(InteropUtils.ToHPtr)}({sourceName});");
            return new TypeConversionResult(targetName, target);
        }

        if (typeof(Delegate).IsAssignableFrom(target) && source.BaseType == typeof(MulticastDelegate))
        {
            var delegateMethodInfo = Utils.GetMulticastDelegateMethodInfo(source);
            targetName = $"{sourceName}Unmanaged";
            var headerSb = new StringBuilder();
            headerSb.Append(typeWriter.GetInteropTypeString(delegateMethodInfo.ReturnType, true));
            headerSb.Append(" ");
            headerSb.Append(targetName);
            headerSb.Append("(");
            var delegateParameters = delegateMethodInfo.GetParameters();
            for (var index = 0; index < delegateParameters.Length; index++)
            {
                var parameterInfo = delegateParameters[index];
                if (index != 0) headerSb.Append(", ");
                headerSb.Append(typeWriter.GetInteropTypeString(parameterInfo.ParameterType));
                headerSb.Append(" ");
                var name = $"arg{index}";
                headerSb.Append(name);
            }

            headerSb.Append(")");
            writer.Write(headerSb.ToString());
            writer.Write("{");
            writer.IncrementIndent();
            
            var invocationSb = new StringBuilder();
            invocationSb.Append(sourceName);
            invocationSb.Append("(");
            
            if (delegateParameters.Length == 0)
            {
                invocationSb.Append(");");
                writer.Write(invocationSb.ToString());
            }
            else
            {
                var appendEmptyLineAfterConversions = false;

                for (var index = 0; index < delegateParameters.Length; index++)
                {
                    var parameterInfo = delegateParameters[index];
                    if (index != 0) invocationSb.Append(", ");
                    var name = $"arg{index}";
                    var result = WriteTypeConversion(writer, preFunctionWriter, parameterInfo.ParameterType, false, name);
                    appendEmptyLineAfterConversions |= name != result.Name;
                    invocationSb.Append(result.Name);
                }

                var hasReturn = delegateMethodInfo.ReturnType != typeof(void);
                if (appendEmptyLineAfterConversions) writer.WriteEmptyLine();
                invocationSb.Append(");");
                if (hasReturn) invocationSb.Insert(0, $"var {targetName}Result = ");
                writer.Write(invocationSb.ToString());
                if (hasReturn)
                {
                    var newName = WriteTypeConversion(writer, preFunctionWriter, delegateMethodInfo.ReturnType, true, $"{targetName}Result");
                    writer.Write("return " + newName.Name + ";");
                }
            }

            writer.DecrementIndent();
            writer.Write("}");

            // With .NET8 this functions is necessary to avoid runtime exception regarding generics with Marshal.GetFunctionPointerForDelegate
            var delegateName = $"{this.methodWrittenDetails.UniqueMethodName}_{targetName}";
            preFunctionWriter.Write($"delegate {headerSb.ToString().Replace(targetName, delegateName)};");

            var targetNameDelegate = $"{targetName}Delegate";
            writer.Write($"{delegateName} {targetNameDelegate} = {targetName};");
            return new TypeConversionResult($"Marshal.GetFunctionPointerForDelegate({targetNameDelegate})", target);
        }


        if (typeof(Delegate).IsAssignableFrom(source) &&  target.BaseType == typeof(MulticastDelegate))
        {
            targetName ??= target.IsGenericType
                ? $"{sourceName}{target.Name.Split('`')[0]}"
                : $"{sourceName}{target.Name}";
            
            void WriteDelegateHeader(Type[] arguments, bool hasReturn)
            {
                //writer.Write($"Console.WriteLine(\"{this.methodWrittenDetails.EntryPoint} function ptr: \" + new IntPtr({sourceName}));"); // helpful for debug
                if (arguments.Length == 0)
                {
                    writer.Write($"{typeWriter.LookupTypeAsText(target)} {targetName} = {sourceName} == null ? default : () => {{");
                }
                else
                {
                    var sb = new StringBuilder();
                    sb.Append(typeWriter.LookupTypeAsText(target));
                    sb.Append(" ");
                    sb.Append(targetName);
                    sb.Append($" = {sourceName} == null ? default : (");
                    for (var index = 0; index < arguments.Length; index++)
                    {
                        if (index != arguments.Length -1 || !hasReturn)
                        {
                            if (index > 0) sb.Append(", ");
                            var name = $"arg{index}";
                            sb.Append(name);
                        }
                    }

                    sb.Append(") => {");
                    writer.Write(sb.ToString());
                    //writer.Write($"Console.WriteLine(\"{this.methodWrittenDetails.EntryPoint} invoking with func ptr: \" + new IntPtr({sourceName}));"); // helpful for debug
                } 
            }
            
            void WriteDelegateBody(Type[] arguments, bool hasReturn)
            {
                var invocationSb = new StringBuilder();
                invocationSb.Append(sourceName);
                invocationSb.Append("(");
                if (arguments.Length == 0)
                {
                    invocationSb.Append(");");
                    writer.Write(invocationSb.ToString());
                    return;
                }

                var appendEmptyLineAfterConversions = false;

                for (var index = 0; index < arguments.Length; index++)
                {
                    var type = arguments[index];
                    if (index != arguments.Length - 1 || !hasReturn)
                    {
                        writer.Write($"InteropUtils.LogDebug($\"Parameter {index} is getting converted now.\");");
                        if (index != 0) invocationSb.Append(", ");
                        var name = $"arg{index}";
                        var result = WriteTypeConversion(writer, preFunctionWriter, type, true, name);
                        appendEmptyLineAfterConversions |= name != result.Name;
                        invocationSb.Append(result.Name);
                        writer.Write($"InteropUtils.LogDebug($\"Parameter {index} for {this.methodWrittenDetails.EntryPoint} has value: {{{result.Name}}}\");");

                    }
                
                    if (index == arguments.Length - 1)
                    {
                        if (appendEmptyLineAfterConversions) writer.WriteEmptyLine();
                        writer.Write($"InteropUtils.LogDebug($\"Invoking handler {{(IntPtr){sourceName}}} for {this.methodWrittenDetails.EntryPoint}\");");
                        invocationSb.Append(");");
                        if (hasReturn) invocationSb.Insert(0, "var result = ");
                        writer.Write(invocationSb.ToString());
                        writer.Write($"InteropUtils.LogDebug($\"Invoked handler {{(IntPtr){sourceName}}} for {this.methodWrittenDetails.EntryPoint}\");");
                        if (hasReturn)
                        {
                            var result = WriteTypeConversion(writer, preFunctionWriter, type, false, "result");
                            writer.Write("return " + result.Name + ";");
                        }
                    }
                }   
            }

            var typeAsString = target.IsGenericType
                ? $"{sourceName}{target.Name.Split('`')[0]}"
                : $"{sourceName}{target.Name}";
            
            if (typeAsString.EndsWith("Action"))
            {
                writer.Write($"InteropUtils.LogDebug($\"Registering action handler {{(IntPtr){sourceName}}} for {this.methodWrittenDetails.EntryPoint}\");");
                WriteDelegateHeader(target.GenericTypeArguments, false);
                writer.IncrementIndent();
                WriteDelegateBody(target.GenericTypeArguments, false);
            }
            else if (typeAsString.EndsWith("Func"))
            {
                writer.Write($"InteropUtils.LogDebug($\"Registering func handler {{(IntPtr){sourceName}}} for {this.methodWrittenDetails.EntryPoint}\");");
                WriteDelegateHeader(target.GenericTypeArguments, true);
                writer.IncrementIndent();
                WriteDelegateBody(target.GenericTypeArguments, true);
            }
            else if (typeAsString.EndsWith("EventHandler"))
            {
                var types = target.GenericTypeArguments;
                if (types.Length == 0)
                {
                    types = new Type[]
                    {
                        typeof(object),
                        typeof(EventArgs)
                    };
                } else if (types.Length == 1)
                {
                    types = new Type[]
                    {
                        typeof(object),
                        types[0]
                    };
                }
                writer.Write($"InteropUtils.LogDebug($\"Registering event handler {{(IntPtr){sourceName}}} for {this.methodWrittenDetails.EntryPoint}\");");
                WriteDelegateHeader(types, false);
                writer.IncrementIndent();
                WriteDelegateBody(types, false);
            }
            else throw new NotSupportedMethodException("Unhandled type");
            writer.DecrementIndent();
            writer.Write("};");
            return new TypeConversionResult(targetName, target);
        }

        if (target == typeof(byte))
        {
            targetName ??= $"{sourceName}Byte";
            writer.Write($"{targetVarCreate}{targetName} = Convert.ToByte({sourceName});");
            return new TypeConversionResult(targetName, target);
        }
        if (target == typeof(bool))
        {
            targetName ??= $"{sourceName}Boolean";
            writer.Write($"{targetVarCreate}{targetName} = Convert.ToBoolean({sourceName});");
            return new TypeConversionResult(targetName, target);
        }

        return new TypeConversionResult(targetName, source);
    }

    private Dictionary<string, string> WriteFunctionHeader(DelayedWriter writer)
    {
        var args = new Dictionary<string, string>();
        writer.Write("[UnmanagedCallersOnly(EntryPoint = \"" + this.methodWrittenDetails.EntryPoint + "\")]");
        
        var sb = new StringBuilder();
        sb.Append("public static ");
        if (IsUnsafeMethod()) sb.Append("unsafe ");
        sb.Append(typeWriter.GetInteropTypeString(this.returnType, true));
        sb.Append(" ");
        sb.Append(this.methodWrittenDetails.UniqueMethodName);
        sb.Append("(");

        var first = true;
        var instanceParameterName = Utils.GetInstanceParameterName(this.methodWrittenDetails.DeclaringType, this.methodBase);
        if (instanceParameterName != null)
        {
            first = false;
            var typeString = typeWriter.GetInteropTypeString(this.methodBase.DeclaringType);
            sb.Append(typeString);
            sb.Append(" ");
            var pName = GetValidParameterName(instanceParameterName);
            sb.Append(pName);
            args.Add(pName, typeString);
        }
        
        foreach (var parameterInfo in this.methodParameterOrder)
        {
            if (!first) sb.Append(", ");
            first = false;
            var typeString = typeWriter.GetInteropTypeString(parameterInfo.ParameterType);
            sb.Append(typeString);
            sb.Append(" ");
            var pName = GetValidParameterName(parameterInfo.Name);
            sb.Append(pName);
            args.Add(pName, typeString);
            if (parameterInfo.IsOptional)
            {
                sb.Append(" = ");
                if (!parameterInfo.HasDefaultValue)
                {
                    sb.Append("default");
                }
                else
                {
                    if (parameterInfo.ParameterType == typeof(bool)) sb.Append("1"); // because we convert it to byte
                    else if (Utils.IsEnum(parameterInfo.ParameterType))
                    {
                        sb.Append(parameterInfo.DefaultValue.GetType().Name);
                        sb.Append(".");
                        sb.Append(parameterInfo.DefaultValue);
                    } 
                    else sb.Append("default");
                }
            }
        }

        sb.Append(")");
        writer.Write(sb.ToString());

        return args;
    }

    private static string GetValidParameterName(string actual)
    {
        if (!CsharpUtils.IsReservedWord(actual)) return actual;
        return "@" + actual;
    }

    private bool IsUnsafeMethod()
    {
        return Utils.GetUsedTypes(this.methodWrittenDetails.DeclaringType, this.methodBase).Any(Utils.IsDelegate);
    }

    
    private string GetMethodName()
    {
        var name = methodBase.Name;
        return (name == ".ctor" ? "Constructor" : name) + nameSuffix;
    }

    private string GetEntryPointName()
    {
        var nameBase = Utils.GetTypeNameForNaming(this.methodWrittenDetails.DeclaringType).ToLowerInvariant();
        var methodName = this.methodWrittenDetails.UniqueMethodName.ToLowerInvariant();
        return $"{nameBase}_{methodName}";
    }

    private class TypeConversionResult
    {
        public TypeConversionResult(string name, Type type)
        {
            this.Name = name;
            this.Type = type;
        }

        public Type Type { get; }

        public string Name { get; }
    }

    private class ReplacedParameterInfo : ParameterInfo
    {
        private readonly object defaultValue;

        public ReplacedParameterInfo(ParameterInfo original, Type newType)
        {
            this.Original = original;
            NewParameterType = newType;
        }

        public ParameterInfo Original { get; }
        public Type NewParameterType { get; }

        public override string Name => Original.Name;
        public override Type ParameterType => NewParameterType;

        public override object DefaultValue => null; // TODO, might need to implement this

        public override bool HasDefaultValue => false; // TODO, might need to implement this
    }
}