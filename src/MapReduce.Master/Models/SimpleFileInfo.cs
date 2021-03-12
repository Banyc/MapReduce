using System.Collections.Generic;

namespace MapReduce.Master.Models
{
    public class SimpleFileInfo
    {
        public int PartitionIndex { get; set; }
        public string FilePath { get; set; }
        public int FileSize { get; set; }
    }
}
