using System.Collections.Generic;
namespace MapReduce.Worker.Helpers
{
    public interface IMapping<TInput, TKey, TValue>
    {
        IList<(TKey, TValue)> Map(TInput input);
    }
}
