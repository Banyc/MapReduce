using System;
namespace MapReduce.Master.Models
{
    public class WorkerInfo
    {
        public string WorkerUuid { get; set; }
        public DateTime LastHeartbeatTime { get; set; }
        public MapReduceTask AssignedTask { get; set; }
    }
}
