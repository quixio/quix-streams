using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Quix.InteropGenerator.Writers.Shared;

public class CsharpWrittenDetails
{
    public List<TypeWrittenDetails> TypeDetails { get; set; } = new List<TypeWrittenDetails>();
    public Assembly Assembly { get; set; }
    public List<Type> Enums { get; set; } = new List<Type>();


    /// <summary>
    /// Extra types that are not from this assembly but its relies on it with public facing method etc
    /// </summary>
    public List<Type> ExtraEnums { get; set; } = new List<Type>();

    /// <summary>
    /// Extra assemblies necessary due to reliance on public facing methods etc
    /// </summary>
    public List<CsharpWrittenDetails> ExternalAssemblies { get; set; } = new List<CsharpWrittenDetails>();
}

public class TypeWrittenDetails
{
    public Type Type { get; set; }

    public List<MethodWrittenDetails> MethodWrittenDetails { get; set; } = new List<MethodWrittenDetails>();
    public string TypeMethodEntryPoint { get; set; }
}

public class MethodWrittenDetails
{
    public string EntryPoint { get; set; }
    public MethodBase MethodBase { get; set; }
    public Type ReturnType { get; set; }
    public ParameterInfo[] ParameterInfos { get; set; }
    public string UniqueMethodName { get; set; }
    
    public Type DeclaringType { get; set; } // Using other than methodbase declaring type to avoid in memory reflected variants, usual for constructors
    
    public Func<Type, bool, string> GetDotNetTypeAsText { get; set; }
    public ParameterInfo[] MethodParameterOrder { get; set; }
}