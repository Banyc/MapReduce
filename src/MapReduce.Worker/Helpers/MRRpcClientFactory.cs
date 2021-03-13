using System.Net.Http;
using Grpc.Net.Client;
using MapReduce.Shared;

namespace MapReduce.Worker.Helpers
{
    public static class MRRpcClientFactory
    {
        public static GrpcChannel CreateGrpcChannel()
        {
            var channel = GrpcChannel.ForAddress("http://localhost:5000");
            return channel;
        }

        public static RpcMapReduceService.RpcMapReduceServiceClient CreaterpcClient(GrpcChannel channel)
        {
            return new RpcMapReduceService.RpcMapReduceServiceClient(channel);
        }
    }
}
