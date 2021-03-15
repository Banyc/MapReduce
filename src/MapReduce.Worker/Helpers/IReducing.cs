using System.Collections.Generic;

namespace MapReduce.Worker.Helpers
{
    public interface IReducing<TKey, TValueIn, TValueOut>
    {
        TValueOut Reduce(TKey key, List<TValueIn> values);
    }
}
