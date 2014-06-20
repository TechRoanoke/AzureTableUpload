using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using AzureTableUpload;
using Newtonsoft.Json;
using System.Threading;
using System.Text;
using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;

namespace WebRole1
{
    public class WebRole : RoleEntryPoint
    {
        public override void Run()
        {
            DoWork();
        }
        public void DoWork()
        {
            // Handle queue processing 

            string acs = RoleEnvironment.GetConfigurationSettingValue("AccountConnectionString");
            string queueName = RoleEnvironment.GetConfigurationSettingValue("QueueName");
            

            CloudStorageAccount account = CloudStorageAccount.Parse(acs);
            CloudQueueClient client = account.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);

            CloudBlobClient clientBlob = account.CreateCloudBlobClient();
            CloudBlobContainer container = clientBlob.GetContainerReference("table-upload-logs");
            container.CreateIfNotExists();

            queue.CreateIfNotExists();

            while (true)
            {
                CloudQueueMessage msg = queue.GetMessage();

                if (msg == null)
                {
                    Thread.Sleep(TimeSpan.FromMinutes(1));
                    continue;
                }

                CancellationTokenSource cts = new CancellationTokenSource();
                Thread t = new Thread(_ =>
                    {
                        while (!cts.Token.IsCancellationRequested)
                        {
                            queue.UpdateMessage(msg, TimeSpan.FromMinutes(10), MessageUpdateFields.Visibility);

                            Thread.Sleep(TimeSpan.FromMinutes(1));
                        }
                    });
                t.Start();

                string payload = msg.AsString;
                WorkItem work = JsonConvert.DeserializeObject<WorkItem>(payload);


                // Log it. Strip private information 
                {
                    WorkItem work2 = JsonConvert.DeserializeObject<WorkItem>(payload);
                    work2.AccountConnectionString = CloudStorageAccount.Parse(work2.AccountConnectionString).Credentials.AccountName;
                    long len = work.GetInputLength();
                    var blobLog = container.GetBlockBlobReference(Path.GetFileNameWithoutExtension(work.InputBlobName) + "-"+ Guid.NewGuid() + "-" + len.ToString() + ".txt");
                    blobLog.UploadText(JsonConvert.SerializeObject(work2, Formatting.Indented));
                }


                // Block in here for hours. This will write an info file that we can read. 
                UploadWrapper(work);

                cts.Cancel();
                t.Join();
                queue.DeleteMessage(msg);
            }
        }

        static void UploadWrapper(WorkItem work)
        {
            try
            {
                Ingress.Upload(work);
            }
            catch (Exception e)
            {
                // Log any errors 
                StringBuilder sb = new StringBuilder();
                sb.AppendLine(e.GetType().FullName);
                sb.AppendLine(e.Message);
                sb.AppendLine(e.ToString());

                string msg = sb.ToString();

                string errBlobName = Path.GetFileNameWithoutExtension(work.InputBlobName) + "-error.txt";
                CloudBlockBlob blobError = work.GetContainer().GetBlockBlobReference(errBlobName);
                blobError.UploadText(msg);
            }
        }

        public override bool OnStart()
        {
            // DoWork();
            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            return base.OnStart();
        }
    }
}
