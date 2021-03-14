using System;
namespace MapReduce.Master.Models
{
    public class WorkerInfo
    {
        public string WorkerUuid { get; set; }
        public DateTime LastHeartBeatTime { get; set; }
        public MapReduceTask AssignedTask { get; set; }
    }
}
