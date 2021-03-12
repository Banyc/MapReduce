using System.Collections.Generic;

namespace MapReduce.Worker.Helpers
{
    public interface IReducing<TKey, TValue>
    {
        TValue Reduce(TKey key, List<TValue> values);
    }
}
