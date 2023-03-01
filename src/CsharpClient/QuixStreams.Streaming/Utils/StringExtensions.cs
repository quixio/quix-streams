using System;
using System.Collections.Generic;
using System.Linq;

namespace QuixStreams.Streaming.Utils
{
    internal static class StringExtensions
    {
        /// <summary>
        /// Returns the amount of keystrokes necessary to change one string to the other
        /// </summary>
        public static int GetLevenshteinDistance(this string a, string b)
        {
            return LevenshteinDistance.Compute(a, b);
        }

        /// <summary>
        /// Returns the strings in the ascending order of keystrokes necessary to change the compared string to the other
        /// </summary>
        public static IEnumerable<string> GetLevenshteinDistance(this string compared, IEnumerable<string> compareTo, int max)
        {
            return compareTo.Select(y => (y.GetLevenshteinDistance(compared), y)).Where(y => y.Item1 <= max).OrderBy(y => y.Item1).Select(y => y.y);
        } 
        
        private static class LevenshteinDistance
        {
            /// <summary>
            /// Compute the distance between two strings.
            /// </summary>
            public static int Compute(string s, string t)
            {
                // Source: https://stackoverflow.com/questions/13793560/find-closest-match-to-input-string-in-a-list-of-strings/13793600
                int n = s.Length;
                int m = t.Length;
                int[,] d = new int[n + 1, m + 1];

                // Step 1
                if (n == 0)
                {
                    return m;
                }

                if (m == 0)
                {
                    return n;
                }

                // Step 2
                for (int i = 0; i <= n; d[i, 0] = i++)
                {
                }

                for (int j = 0; j <= m; d[0, j] = j++)
                {
                }

                // Step 3
                for (int i = 1; i <= n; i++)
                {
                    //Step 4
                    for (int j = 1; j <= m; j++)
                    {
                        // Step 5
                        int cost = (t[j - 1] == s[i - 1]) ? 0 : 1;

                        // Step 6
                        d[i, j] = Math.Min(
                            Math.Min(d[i - 1, j] + 1, d[i, j - 1] + 1),
                            d[i - 1, j - 1] + cost);
                    }
                }
                // Step 7
                return d[n, m];
            }
        }
    }
    
    
}