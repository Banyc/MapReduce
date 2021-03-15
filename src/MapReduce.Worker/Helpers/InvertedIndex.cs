using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace MapReduce.Worker.Helpers
{
    public class InvertedIndex : IMapping<string, List<object>>, IReducing<string, List<object>, List<List<object>>>
    {
        public async Task<IList<(string, List<object>)>> MapAsync(FileStream inputFile)
        {
            string fileName = Path.GetFileName(inputFile.Name);
            using var streamReader = new StreamReader(inputFile);
            string text = await streamReader.ReadToEndAsync().ConfigureAwait(false);
            string[] tokens = text.Split();
            List<(string, List<object>)> mappings = new();
            int i;
            for (i = 0; i < tokens.Length; i++)
            {
                mappings.Add(
                    (tokens[i], new List<object>{ fileName, i })
                );
            }
            return mappings;
        }

        public List<List<object>> Reduce(string key, List<List<object>> values)
        {
            return values;
        }
    }
}
