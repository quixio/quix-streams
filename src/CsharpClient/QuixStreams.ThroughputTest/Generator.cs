using System;
using System.Collections.Generic;
using System.Text;

namespace QuixStreams.ThroughputTest
{
    public class Generator
    {
        private Random random;

        public Generator()
        {
            this.random = new Random(123456789); // this is quite important, we don't want random data but same random data across multiple runs
        }

        public IEnumerable<string> GenerateParameters(int count)
        {
            var buffer = new byte[10];
            for (int i = 0; i < count; i++)
            {
                random.NextBytes(buffer);
                yield return Convert.ToBase64String(buffer);
            }
        }

        public string GenerateStringValue(int length)
        {
            var buffer = new byte[length];
            return Encoding.UTF8.GetString(buffer);
        }

        public double GenerateNumericValue()
        {
            return random.NextDouble() * random.Next(0, 50000);
        }

        public bool HasValue()
        {
            return random.Next(0, 4) != 0; // 25% chance to not have value
        }
    }
}