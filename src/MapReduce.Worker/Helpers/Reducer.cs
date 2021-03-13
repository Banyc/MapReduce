using System.Linq;
using System;
using System.IO;
using System.Collections.Generic;
using MapReduce.Shared;
using System.Threading.Tasks;
using MapReduce.Worker.Models;

namespace MapReduce.Worker.Helpers
{
    public class Reducer<TKey, TValue>
    {
        private readonly IReducing<TKey, TValue> _reducingPhase;
        private readonly WorkerSettings _settings;

        public Reducer(IReducing<TKey, TValue> reducingPhase, WorkerSettings settings)
        {
            _reducingPhase = reducingPhase;
            _settings = settings;
        }

        private static async Task<Dictionary<TKey, List<TValue>>> ReadMappingsAsync(List<string> filePaths)
        {
            Dictionary<TKey, List<TValue>> mappings = new();

            foreach (var filePath in filePaths)
            {
                using var fs = File.OpenRead(filePath);
                using var sr = new StreamReader(fs);

                Dictionary<TKey, List<TValue>> temp =
                    await System.Text.Json.JsonSerializer
                        .DeserializeAsync<Dictionary<TKey, List<TValue>>>(fs).ConfigureAwait(false);

                foreach (var tempKeyValue in temp)
                {
                    if (mappings.ContainsKey(tempKeyValue.Key))
                    {
                        mappings[tempKeyValue.Key].AddRange(tempKeyValue.Value);
                    }
                    else
                    {
                        mappings[tempKeyValue.Key] = tempKeyValue.Value;
                    }
                }
            }

            return mappings;
        }

        public async Task StartAsync(List<string> intermediateFilePaths, int taskId, int partitionIndex)
        {
            // read intermediates from files
            var mappings = await ReadMappingsAsync(intermediateFilePaths).ConfigureAwait(false);

            // reduce
            Dictionary<TKey, TValue> reduced = new();
            foreach (var keyValues in mappings)
            {
                TValue reducedValue = _reducingPhase.Reduce(keyValues.Key, keyValues.Value);
                reduced[keyValues.Key] = reducedValue;
            }

            // save intermediate to file
            FileInfoDto fileInfo = new();
            string tempFileName = $"mr-{taskId}-{partitionIndex}";
            using var tempFileStream = File.OpenWrite(tempFileName);
            await System.Text.Json.JsonSerializer.SerializeAsync(tempFileStream, reduced).ConfigureAwait(false);
            fileInfo.FileSize = (int)tempFileStream.Length;
            fileInfo.FilePath = tempFileStream.Name;

            // report to master
            var grpcClient = MRGrpcClientFactory.CreateMRGrpcClient();
            await grpcClient.ReduceDoneAsync(new() {
                FileInfo = fileInfo,
                TaskId = taskId,
                WorkerInfo = new()
                {
                    WorkerUuid = _settings.WorkerUuid
                }
            });
        }
    }
}
