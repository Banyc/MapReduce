using System.Collections.Generic;

namespace MapReduce.Master.Models
{
    public class MasterSettings
    {
        public string IpAddress { get; set; }
        public int Port { get; set; }
        public int ReduceTaskCount { get; set; }
        public List<string> InputFilePaths { get; set; }
    }
}
