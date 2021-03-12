using System.IO;
using System.Collections.Generic;
namespace MapReduce.Master.Models
{
    public class MapTask : MapReduceTask
    {
        // public string AssignedFilePath { get; set; }
        public FileInfo AssignedFile { get; set; }
        public IList<SimpleFileInfo> CompletedFileInfos { get; set; }
        public override bool IsTaskCompleted { get => this.CompletedFileInfos != null; }
    }
}
