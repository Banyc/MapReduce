using System.Collections.Generic;
namespace MapReduce.Worker.Helpers
{
    public interface IPartitioning<TKey, TValue>
    {
        Dictionary<int, Dictionary<TKey, List<TValue>>> Partition(Dictionary<TKey, List<TValue>> mappingsMerged, int numPartitions);
    }
}
