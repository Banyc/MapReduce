using System.Collections.Generic;
using System.Linq;

namespace MapReduce.Worker.Helpers
{
    public class WordCount : IMapping<string, string, int>, IReducing<string, int>
    {
        public IList<(string, int)> Map(string input)
        {
            var tokens = input.Split();
            List<(string, int)> mappings = new();
            foreach (var token in tokens)
            {
                mappings.Add((token, 1));
            }
            return mappings;
        }

        public int Reduce(string key, List<int> values)
        {
            int reduced = 0;
            foreach (var item in values)
            {
                reduced += item;
            }
            return reduced;
        }
    }
}
