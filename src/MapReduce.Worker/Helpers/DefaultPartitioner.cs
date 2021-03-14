using System.Collections.Generic;

namespace MapReduce.Worker.Helpers
{
    public class DefaultPartitioner<TKey, TValue> : IPartitioning<TKey, TValue>
    {
        public Dictionary<int, Dictionary<TKey, List<TValue>>> Partition(Dictionary<TKey, List<TValue>> mappingsMerged, int numPartitions)
        {
            Dictionary<int, Dictionary<TKey, List<TValue>>> partitions = new();
            foreach (var keyValues in mappingsMerged)
            {
                int partitionNumber = (keyValues.Key.GetHashCode() & 0x7fffffff) % numPartitions;
                if (!partitions.ContainsKey(partitionNumber))
                {
                    partitions[partitionNumber] = new();
                }
                partitions[partitionNumber][keyValues.Key] = keyValues.Value;
            }
            return partitions;
        }
    }
}
