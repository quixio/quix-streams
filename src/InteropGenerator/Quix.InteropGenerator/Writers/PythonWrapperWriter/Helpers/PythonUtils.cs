using System.Collections.Generic;

namespace Quix.InteropGenerator.Writers.PythonWrapperWriter.Helpers;

public class PythonUtils
{
    private static HashSet<string> reservedWords = new HashSet<string>()
    {
        "False", "def", "if", "raise", "None", "del", "import", "return", "True", "elif", "in", "try", "and", "else",
        "is", "while", "as", "except", "lambda", "with", "assert", "finally", "nonlocal", "yield", "break", "for",
        "not", "", "class", "from", "or", "", "continue", "global", "pass"
    };

    public static bool IsReservedWord(string word)
    {
        return reservedWords.Contains(word);
    }
}