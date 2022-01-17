using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Grpc.Core;
using Grpc.Net.Client;
using MapReduce.Shared;
using MapReduce.Shared.Helpers;
using MapReduce.Worker.Models;

namespace MapReduce.Worker.Helpers
{
    public class Worker<TKey, TValueIn, TValueOut> : IDisposable
    {
        private readonly WorkerInfoDto _workerInfoDto;
        private readonly WorkerSettings _settings;

        private readonly IMapping<TKey, TValueIn> _mappingPhase;
        private readonly IReducing<TKey, TValueIn, TValueOut> _reducingPhase;
        private readonly IPartitioning<TKey, TValueIn> _partitioningPhase;
        private readonly GrpcChannel _channel;
        private readonly System.Timers.Timer _heartBeatTicker;
        public bool IsWorking { get; private set; }

        public Worker(
            WorkerSettings settings,
            RpcClientFactory rpcClientFactory,
            IMapping<TKey, TValueIn> mappingPhase,
            IReducing<TKey, TValueIn, TValueOut> reducingPhase,
            IPartitioning<TKey, TValueIn> partitioningPhase)
        {
            _settings = settings;
            _mappingPhase = mappingPhase;
            _reducingPhase = reducingPhase;
            _partitioningPhase = partitioningPhase;
            _workerInfoDto = new()
            {
                WorkerUuid = settings.WorkerUuid
            };
            _channel = rpcClientFactory.CreateRpcChannel();
            _heartBeatTicker = new()
            {
                Interval = TimeSpan.FromSeconds(4).TotalMilliseconds
            };
            StartHeartbeat();
        }

        public void Dispose()
        {
            _heartBeatTicker.Dispose();
            _channel.Dispose();
        }

        public void StartHeartbeat()
        {
            // heart beats
            var rpcClientHeartbeat = RpcClientFactory.CreateRpcClient(_channel);

            _heartBeatTicker.Elapsed += (object sender, ElapsedEventArgs e) => _ = SendHeartbeatAsync(rpcClientHeartbeat);
            _heartBeatTicker.Start();
        }

        public async Task RunAsync(CancellationToken cancelToken)
        {
            // worker tasks
            if (this.IsWorking)
            {
                return;
            }
            this.IsWorking = true;

            var rpcClientWorkTask = RpcClientFactory.CreateRpcClient(_channel);
            var workTask = Task.Run(() => WorkLoopAsync(rpcClientWorkTask, cancelToken), cancelToken);

            try
            {
                await Task.WhenAll(workTask).ConfigureAwait(false);
            }
            finally
            {
                this.IsWorking = false;
            }
        }

        private async Task SendHeartbeatAsync(
            RpcMapReduceService.RpcMapReduceServiceClient rpcClient)
        {
            try
            {
                _ = await rpcClient.HeartbeatAsync(_workerInfoDto);
            }
            catch (RpcException) { }
        }

        private async Task WorkLoopAsync(
            RpcMapReduceService.RpcMapReduceServiceClient rpcClient, CancellationToken cancelToken)
        {
            while (!cancelToken.IsCancellationRequested)
            {
                // fetch task from master
                try
                {
                    var taskInfoDto = await rpcClient.AskForTaskAsync(_workerInfoDto, cancellationToken: cancelToken);

                    switch ((MapReduceTaskType)taskInfoDto.TaskType)
                    {
                        case MapReduceTaskType.Map:
                            Mapper<TKey, TValueIn> mapper = new(
                                mappingPhase: _mappingPhase,
                                partitioningPhase: _partitioningPhase,
                                rpcClient: rpcClient,
                                settings: _settings);
                            await mapper.StartAsync(
                                inputFilePath: taskInfoDto.InputFileInfo.FilePath,
                                taskId: taskInfoDto.TaskId,
                                numPartitions: taskInfoDto.ReduceTaskCount
                            ).ConfigureAwait(false);
                            break;
                        case MapReduceTaskType.Reduce:
                            Reducer<TKey, TValueIn, TValueOut> reducer = new(
                                reducingPhase: _reducingPhase,
                                rpcClient: rpcClient,
                                settings: _settings);
                            await reducer.StartAsync(
                                intermediateFilePaths: taskInfoDto.IntermediateFilesInfos.Select(xxxx => xxxx.FilePath).ToList(),
                                taskId: taskInfoDto.TaskId,
                                partitionIndex: taskInfoDto.PartitionIndex
                            ).ConfigureAwait(false);
                            break;
                        case MapReduceTaskType.Nop:
                        case MapReduceTaskType.Exit:
                        default:
                            await Task.Delay(TimeSpan.FromSeconds(4), cancelToken).ConfigureAwait(false);
                            break;
                    }
                }
                catch (RpcException)
                {
                    await Task.Delay(TimeSpan.FromSeconds(4), cancelToken).ConfigureAwait(false);
                }
            }
        }
    }
}
