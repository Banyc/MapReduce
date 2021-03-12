using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using MapReduce.Shared;
using System.Threading.Tasks;
using MapReduce.Worker.Models;

namespace MapReduce.Worker.Helpers
{
    public class Mapper<TInput, TKey, TValue>
    {
        private readonly IMapping<TInput, TKey, TValue> _mappingPhase;
        private readonly IPartitioning<TKey, TValue> _partitioningPhase;
        private readonly WorkerSettings _settings;
        private readonly RpcMapReduceService.RpcMapReduceServiceClient _grpcClient;

        public Mapper(IMapping<TInput, TKey, TValue> mappingPhase, IPartitioning<TKey, TValue> partitioningPhase, WorkerSettings settings)
        {
            _mappingPhase = mappingPhase;
            _partitioningPhase = partitioningPhase;
            _settings = settings;
            _grpcClient = MRGrpcClientFactory.CreateMRGrpcClient();
        }

        // fetch task from master

        public async Task StartAsync(TInput input, int taskId, int numPartitions)
        {
            var mappings = _mappingPhase.Map(input);

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

            foreach (var partition in partitions)
            {
                FileInfoDto fileInfo = new();

                string fileName = $"mr-temp-{taskId}-{partition.Key}";

                using FileStream tempFileStream = File.OpenWrite(fileName);
                // using StreamWriter sw = new(tempFileStream);
                // string json = System.Text.Json.JsonSerializer.Serialize(partition.Value);
                // sw.Write(json);
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
            await _grpcClient.MapDoneAsync(message);
        }
    }
}
