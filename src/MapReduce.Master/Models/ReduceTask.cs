using System.Collections.Generic;

namespace MapReduce.Master.Models
{
    public class ReduceTask : MapReduceTask
    {
        public IList<SimpleFileInfo> AssignedFiles { get; set; }
        public SimpleFileInfo CompletedFileInfo { get; set; }
        public override bool IsTaskCompleted { get => this.CompletedFileInfo != null; }
    }
}
