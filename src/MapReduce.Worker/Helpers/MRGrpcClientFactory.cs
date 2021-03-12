using System.Net.Http;
using Grpc.Net.Client;
using MapReduce.Shared;

namespace MapReduce.Worker.Helpers
{
    public static class MRGrpcClientFactory
    {
        public static RpcMapReduceService.RpcMapReduceServiceClient CreateMRGrpcClient()
        {
            var httpHandler = new HttpClientHandler();

            using var channel = GrpcChannel.ForAddress("http://localhost:5000");

            var client = new RpcMapReduceService.RpcMapReduceServiceClient(channel);
            return client;
        }
    }
}
