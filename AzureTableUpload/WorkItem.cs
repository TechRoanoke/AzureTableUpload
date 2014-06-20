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
    public class WorkItem
    {
        // The account that the table and blobs live in. 
        public string AccountConnectionString { get; set; }
        // Table name to create
        public string TableName { get; set; }

        // Identify the Partition and Row Key name in the column set
        public string PartKeyName { get; set; }
        public string RowKeyName { get; set; }
        
        // Input is eitehr from a blob or from a local filename
        // Blob input source
        public string InputContainerName { get; set; }
        public string InputBlobName { get; set; }

        // If this ends in .zip, we assume it's compressed and will unzip on the server.
        public string InputFilename { get; set; }

        public string GetAccountName()
        {
            return CloudStorageAccount.Parse(AccountConnectionString).Credentials.AccountName;
        }

        public CloudBlobContainer GetContainer()
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(this.AccountConnectionString);
            CloudBlobClient clientBlob = account.CreateCloudBlobClient();

            CloudBlobContainer container = clientBlob.GetContainerReference(this.InputContainerName);

            container.CreateIfNotExists();
            //if (!container.Exists())
            //{
                //throw new InvalidOperationException("Container '" + InputContainerName + "' does not exist in account.");
            //}
            return container;
        }

        public ICloudBlob GetBlob()
        {
            var container = GetContainer();
            ICloudBlob blobInput = container.GetBlobReferenceFromServer(InputBlobName);
            if (!blobInput.Exists())
            {
                throw new InvalidOperationException("Blob '" + InputBlobName + "' does not exist in container '" + InputContainerName + "'.");
            }
            return blobInput;
        }

        // Get info blob. This won't exist if we haven't started the upload yet. 
        // The info blob is an Info object that gives us real-time upload stats. 
        public CloudBlockBlob GetProgressStatusBlob()
        {
            string infoName = Path.GetFileNameWithoutExtension(InputBlobName) + "-info.txt";
            CloudBlockBlob blobInfo = GetContainer().GetBlockBlobReference(infoName);
            return blobInfo;
        }

        // Get the Info object/ This gets updated as we make progress and then re-uploaded.
        public ProgressStatus GetProgressStatus()
        {
            var blobInfo = GetProgressStatusBlob();
            if (blobInfo.Exists())
            {
                var time = blobInfo.Properties.LastModified;

                string json = blobInfo.DownloadText();
                var info = JsonConvert.DeserializeObject<ProgressStatus>(json);
                info.LastModifieed = time;
                return info;
            }
            return null;
        }

        public void UpdateProgressStatus(ProgressStatus info)
        {
            string json = JsonConvert.SerializeObject(info, Formatting.Indented);
            var blobInfo = GetProgressStatusBlob();
            blobInfo.UploadText(json);
        }

        public Stream OpenStream()
        {
            if (this.InputFilename != null)
            {
                return new FileStream(InputFilename, FileMode.Open, FileAccess.Read);
            }

            var blobInput = GetBlob();
            Stream streamDirect = blobInput.OpenRead();
            Stream stream = new BufferedStream(streamDirect, bufferSize: 64 * 1024);

            return stream;            
        }

        public long GetInputLength()
        {
            if (this.InputFilename != null)
            {
                long len = new FileInfo(this.InputFilename).Length;
                return len;
            }
            var blob = GetBlob();
            return blob.Properties.Length;
        }

        public string GetETag()
        {
            if (this.InputFilename != null)
            {
                return null; // no etag for files. $$$ Use timestamp?
            }
            return GetBlob().Properties.ETag;
        }

        [JsonIgnore]
        // Is the input file compressed?
        public bool IsCompressed
        {
            get
            {
                return this.InputBlobName.EndsWith(".zip", StringComparison.OrdinalIgnoreCase);
            }
        }      
    }
}