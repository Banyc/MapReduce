using System.Net.Http;
using Grpc.Net.Client;
using MapReduce.Shared;
using MapReduce.Worker.Models;

namespace MapReduce.Worker.Helpers
{
    public class RpcClientFactory
    {
        private readonly RpcClientFactorySettings _settings;

        public RpcClientFactory(RpcClientFactorySettings settings)
        {
            _settings = settings;
        }

        public GrpcChannel CreateRpcChannel()
        {
            var channel = GrpcChannel.ForAddress(_settings.Address);
            return channel;
        }

        public static RpcMapReduceService.RpcMapReduceServiceClient CreateRpcClient(GrpcChannel channel)
        {
            return new RpcMapReduceService.RpcMapReduceServiceClient(channel);
        }
    }
}
