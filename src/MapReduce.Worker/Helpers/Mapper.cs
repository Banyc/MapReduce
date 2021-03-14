using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using MapReduce.Shared;
using System.Threading.Tasks;
using MapReduce.Worker.Models;

namespace MapReduce.Worker.Helpers
{
    public class Mapper<TKey, TValue>
    {
        private readonly IMapping<TKey, TValue> _mappingPhase;
        private readonly IPartitioning<TKey, TValue> _partitioningPhase;
        private readonly WorkerSettings _settings;
        private readonly RpcMapReduceService.RpcMapReduceServiceClient _rpcClient;

        public Mapper(
            IMapping<TKey, TValue> mappingPhase,
            IPartitioning<TKey, TValue> partitioningPhase,
            RpcMapReduceService.RpcMapReduceServiceClient rpcClient,
            WorkerSettings settings)
        {
            _mappingPhase = mappingPhase;
            _partitioningPhase = partitioningPhase;
            _settings = settings;
            _rpcClient = rpcClient;
        }

        public async Task StartAsync(string inputFilePath, int taskId, int numPartitions)
        {
            // map
            IList<(TKey, TValue)> mappings = null;
            using (var fileStream = File.OpenRead(inputFilePath))
            {
                mappings = await _mappingPhase.MapAsync(fileStream).ConfigureAwait(false);
            }

            // merge
            Dictionary<TKey, List<TValue>> mappingsMerged = new();

            foreach (var mapping in mappings)
            {
                if (mappingsMerged.ContainsKey(mapping.Item1))
                {
                    mappingsMerged[mapping.Item1].Add(mapping.Item2);
                }
                else
                {
                    mappingsMerged[mapping.Item1] = new() { mapping.Item2 };
                }
            }

            // partition
            var partitions = _partitioningPhase.Partition(mappingsMerged, numPartitions);
            List<FileInfoDto> fileInfos = new();

            // save each partition to a file
            foreach (var partition in partitions)
            {
                FileInfoDto fileInfo = new();

                string fileName = $"mr-temp-{taskId}-{partition.Key}";
                Directory.CreateDirectory(_settings.MappedOutputDirectory);
                string path = Path.Combine(_settings.MappedOutputDirectory, fileName);
                using FileStream tempFileStream = File.OpenWrite(path);
                await System.Text.Json.JsonSerializer.SerializeAsync(tempFileStream, partition.Value).ConfigureAwait(false);

                fileInfo.FileSize = (int)tempFileStream.Length;
                fileInfo.FilePath = tempFileStream.Name;
                fileInfos.Add(fileInfo);
            }

            // report to master
            MapOutputInfoDto message = new()
            {
                TaskId = taskId,
                WorkerInfo = new()
                {
                    WorkerUuid = _settings.WorkerUuid
                }
            };
            message.FileInfos.AddRange(fileInfos);
            await _rpcClient.MapDoneAsync(message);
        }
    }
}
