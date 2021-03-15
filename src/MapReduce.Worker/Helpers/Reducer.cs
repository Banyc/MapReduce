using System.Linq;
using System;
using System.IO;
using System.Collections.Generic;
using MapReduce.Shared;
using System.Threading.Tasks;
using MapReduce.Worker.Models;

namespace MapReduce.Worker.Helpers
{
    public class Reducer<TKey, TValueIn, TValueOut>
    {
        private readonly IReducing<TKey, TValueIn, TValueOut> _reducingPhase;
        private readonly RpcMapReduceService.RpcMapReduceServiceClient _rpcClient;
        private readonly WorkerSettings _settings;

        public Reducer(
            IReducing<TKey, TValueIn, TValueOut> reducingPhase,
            RpcMapReduceService.RpcMapReduceServiceClient rpcClient,
            WorkerSettings settings)
        {
            _reducingPhase = reducingPhase;
            _rpcClient = rpcClient;
            _settings = settings;
        }

        private static async Task<Dictionary<TKey, List<TValueIn>>> ReadMappingsAsync(List<string> filePaths)
        {
            Dictionary<TKey, List<TValueIn>> mappings = new();

            foreach (var filePath in filePaths)
            {
                using var fs = File.OpenRead(filePath);
                using var sr = new StreamReader(fs);

                Dictionary<TKey, List<TValueIn>> temp =
                    await System.Text.Json.JsonSerializer
                        .DeserializeAsync<Dictionary<TKey, List<TValueIn>>>(fs)
                        .ConfigureAwait(false);

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
            Dictionary<TKey, TValueOut> reduced = new();
            foreach (var keyValues in mappings)
            {
                TValueOut reducedValue = _reducingPhase.Reduce(keyValues.Key, keyValues.Value);
                reduced[keyValues.Key] = reducedValue;
            }

            // save intermediate to file
            FileInfoDto fileInfo = new();
            string tempFileName = $"mr-{taskId}-{partitionIndex}";
            Directory.CreateDirectory(_settings.ReducedOutputDirectory);
            string path = Path.Combine(_settings.ReducedOutputDirectory, tempFileName);
            File.Delete(path);  // clean the whole file before pure overwrite
            using var tempFileStream = File.OpenWrite(path);
            await System.Text.Json.JsonSerializer.SerializeAsync(tempFileStream, reduced).ConfigureAwait(false);
            fileInfo.FileSize = (int)tempFileStream.Length;
            fileInfo.FilePath = tempFileStream.Name;
            fileInfo.PartitionIndex = partitionIndex;

            // report to master
            await _rpcClient.ReduceDoneAsync(new() {
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
