using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

namespace MapReduce.Worker.Helpers
{
    public class InvertedIndex : IMapping<string, List<object>>, IReducing<string, List<object>, Dictionary<string, List<int>>>
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
                    (tokens[i], new List<object> { fileName, i })
                );
            }
            return mappings;
        }

        public Dictionary<string, List<int>> Reduce(string key, List<List<object>> values)
        {
            Dictionary<string, List<int>> result = new();
            foreach (var docPosPair in values)
            {
                string docName = ((JsonElement)docPosPair[0]).GetString();
                int positionInDoc = ((JsonElement)docPosPair[1]).GetInt32();
                if (result.ContainsKey(docName))
                {
                    result[docName].Add(positionInDoc);
                }
                else
                {
                    result[docName] = new() { positionInDoc };
                }
            }
            return result;
        }
    }
}
