using System.IO;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MapReduce.Worker.Helpers
{
    public interface IMapping<TKey, TValue>
    {
        Task<IList<(TKey, TValue)>> MapAsync(FileStream inputFile);
    }
}
