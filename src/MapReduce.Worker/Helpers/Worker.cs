using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using MapReduce.Shared;
using MapReduce.Shared.Helpers;
using MapReduce.Worker.Models;

namespace MapReduce.Worker.Helpers
{
    public class Worker
    {
        private readonly string _workerUuid = Guid.NewGuid().ToString();
        private readonly WorkerInfoDto _workerInfoDto;
        private readonly WorkerSettings _settings;

        public Worker(WorkerSettings settings)
        {
            _settings = settings;
            _workerInfoDto = new()
            {
                WorkerUuid = _workerUuid
            };
        }

        public async Task StartAsync(CancellationToken cancelToken)
        {
            var grpcClient = MRGrpcClientFactory.CreateMRGrpcClient();

            var heartBeatTask = Task.Run(() => HeartBeatLoopAsync(grpcClient, cancelToken));
            var workTask = Task.Run(() => WorkLoopAsync(grpcClient, cancelToken));

            await Task.WhenAll(heartBeatTask, workTask).ConfigureAwait(false);
        }

        private async Task HeartBeatLoopAsync(
            RpcMapReduceService.RpcMapReduceServiceClient grpcClient, CancellationToken cancelToken)
        {
            while (!cancelToken.IsCancellationRequested)
            {
                try
                {
                    _ = await grpcClient.HeartBeatAsync(_workerInfoDto, cancellationToken: cancelToken);
                }
                catch (RpcException) { }

                await Task.Delay(TimeSpan.FromSeconds(4), cancelToken).ConfigureAwait(false);
            }
        }

        private async Task WorkLoopAsync(
            RpcMapReduceService.RpcMapReduceServiceClient grpcClient, CancellationToken cancelToken)
        {
            while (!cancelToken.IsCancellationRequested)
            {
                // fetch task from master
                try
                {
                    var taskInfoDto = await grpcClient.AskForTaskAsync(_workerInfoDto, cancellationToken: cancelToken);

                    WordCount wordCount = new();

                    switch ((MapReduceTaskType)taskInfoDto.TaskType)
                    {
                        case MapReduceTaskType.Map:
                            Mapper<string, int> mapper = new(
                                mappingPhase: wordCount,
                                partitioningPhase: new DefaultPartitioner<string, int>(),
                                settings: _settings);
                            await mapper.StartAsync(
                                inputFilePath: taskInfoDto.InputFileInfo.FilePath,
                                taskId: taskInfoDto.TaskId,
                                numPartitions: taskInfoDto.ReduceTaskCount
                            ).ConfigureAwait(false);
                            break;
                        case MapReduceTaskType.Reduce:
                            Reducer<string, int> reducer = new(
                                reducingPhase: wordCount,
                                settings: _settings);
                            await reducer.StartAsync(
                                intermediateFilePaths: taskInfoDto.IntermediateFilesInfos.Select(xxxx => xxxx.FilePath).ToList(),
                                taskId: taskInfoDto.TaskId,
                                partitionIndex: taskInfoDto.PartitionIndex
                            ).ConfigureAwait(false);
                            break;
                        case MapReduceTaskType.Exit:
                        default:
                            await Task.Delay(TimeSpan.FromSeconds(4), cancelToken).ConfigureAwait(false);
                            break;
                    }
                }
                catch (RpcException) {
                    await Task.Delay(TimeSpan.FromSeconds(4), cancelToken).ConfigureAwait(false);
                }
            }
        }
    }
}
