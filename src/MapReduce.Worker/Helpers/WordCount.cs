using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MapReduce.Worker.Helpers
{
    public class WordCount : IMapping<string, int>, IReducing<string, int>
    {
        public async Task<IList<(string, int)>> MapAsync(FileStream inputFile)
        {
            using var sr = new StreamReader(inputFile);
            string input = await sr.ReadToEndAsync().ConfigureAwait(false);
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
