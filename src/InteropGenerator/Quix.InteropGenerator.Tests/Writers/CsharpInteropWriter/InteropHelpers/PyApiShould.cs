using FluentAssertions;
using InteropHelpers.Interop;
using Xunit;

namespace Quix.InteropGenerator.Tests.Writers.CsharpInteropWriter.InteropHelpers;

public class PyApiShould
{
    private PyApi3 CreatePyApi()
    {
        var pyApi = new PyApi3(null);
        return pyApi;
    }
    
    [Fact]
    public void InitApy_ShouldHavePythonInitialized()
    {
        // Arrange
        var pyApi = CreatePyApi();
        
        // Assert
        pyApi.IsInitialized().Should().BeTrue();
    }
}