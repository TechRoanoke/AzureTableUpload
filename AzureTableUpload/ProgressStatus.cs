using DataAccess;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableUpload
{
    // Describes checkpoint table for long-running table upload operation. 
    public class ProgressStatus
    {
        public int TotalUploaded { get; set; }
        public int TotalSkipped { get; set; }
        public int TotalRead { get; set; }
        public long TimeInRead { get; set; }
        public long TimeInUpload { get; set; }

        // Non-zero if this is a resume operation 
        public int StartRow { get; set; }
        public int CountUploadedThisSession { get; set; }
        public int SessionCounter { get; set; }

        public string InputBlobETag { get; set; }

        public bool Completed { get; set; }

        // Length of the input blob in bytes. Useful for progress counter
        public long InputLength { get; set; }
        public long Progress { get; set; }

        // UTC Timestamp for when this was modified. 
        public DateTimeOffset? LastModifieed { get; set; }
    }
}