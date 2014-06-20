using DataAccess;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
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
    public class Program
    {
        internal static Config _config;

        static void Main(string[] args)
        {
            CommandArgs opts = CommandArgs.Parse(args);
            if (opts == null)
            {
                return;
            }

            switch (opts.Mode)
            {
                case Mode.Validate:
                    Validate(opts.Work);
                    break;

                case Mode.Status:
                    PrintStatus(opts.Work);
                    break;

                case Mode.UploadLocal:
                    Validate(opts.Work);
                    Ingress.Upload(opts.Work);

                    // Succeeded, print results 
                    PrintStatus(opts.Work);
                    break;

                case Mode.UploadServer:
                    QueueToServer(opts.Work);
                    break;
            }
        }
        
        // Validate whether a table can be uploaded, without actually uploading it. 
        static void Validate(WorkItem work)
        {
            int i = 0;

            int illegal = 0;
            int dups = 0;

            // Map of PartKey-->HashSet<RowKey>.  Used to determine spread & uniqueness.
            Dictionary<string, HashSet<string>> unique = new Dictionary<string, HashSet<string>>();

            using (Stream stream = work.OpenStream())
            {
                var dt = DataTable.New.ReadLazy(stream);
                Ingress.Init(work, dt);                

                foreach (var row in dt.Rows)
                {
                    string partKey = Ingress.GetPartitionKey(row);
                    string rowKey = Ingress.GetRowKey(row);

                    if (HasIllegalChars(partKey) || HasIllegalChars(rowKey))
                    {
                        illegal++;
                    }

                    HashSet<string> rowKeys;
                    if (unique.TryGetValue(partKey, out rowKeys))
                    {
                        if (rowKeys.Add(rowKey))
                        {
                            // Added
                        }
                        else
                        {
                            // Already present
                            dups++;
                        }
                    }
                    else
                    {
                        rowKeys = new HashSet<string>();
                        rowKeys.Add(rowKey);
                        unique[partKey] = rowKeys;
                    }
                    i++;

                    if (i % 10000 == 0)
                    {
                        Console.Write(".");
                    }
                }
            }

            Console.WriteLine();
            Console.WriteLine("Total rows: {0}", i);
            Console.WriteLine("  Unique partition keys: {0}, avg row keys per part={1}", unique.Count, ((double) i) / unique.Count);
            if (illegal > 0)
            {
                Console.WriteLine("  {0} rows with illegal characters \\/#?", illegal);
            }
        }

        private static void QueueToServer(WorkItem workItem)
        {
            // Validate locally first. 
            Validate(workItem);

            // Upload the blob (if it's not there yet) 
            var container = workItem.GetContainer();
            var blob = container.GetBlockBlobReference(workItem.InputBlobName);
            if (!blob.Exists())
            {
                Console.WriteLine("Uploading the file to a blob ... {0} bytes", workItem.GetInputLength());
                blob.UploadFromFile(workItem.InputFilename, FileMode.Open);
                Console.WriteLine("blob upload finished");
            }

            // - zip the blob $$$ 
            
            // queue a message so the server can process it
            var queue = _config.GetQueue();

            // Clear out local filename (since server will be reading it from blob instead) 
            workItem.InputFilename = null;

            string json = JsonConvert.SerializeObject(workItem, Formatting.Indented);
            queue.AddMessage(new CloudQueueMessage(json));

            Console.WriteLine("Message has been queued to {0}", queue.Name);
            Console.WriteLine("You can see the progress via the -status switch");
        }

        static void PrintStatus(WorkItem item)
        {
            Console.WriteLine("Status for table upload {0}:{1}", item.GetAccountName(), item.TableName);
            var info = item.GetProgressStatus();
            if (info == null)
            {
                Console.WriteLine("No upload started yet");
                Console.WriteLine("Run with -upload switch to begin an upload");
                return;
            }

            if (info.LastModifieed.HasValue)
            {
                TimeSpan age = DateTime.UtcNow - info.LastModifieed.Value;
                Console.WriteLine(" last updated {0:n0} seconds ago.", age.TotalSeconds);
            }

            if (info.Completed)
            {
                Log.WriteLine(ConsoleColor.Green, "Upload is successful!");
            }
            else
            {
                Log.WriteLine(ConsoleColor.Yellow, "In progress: {0:n0}/{1:n0} bytes {2}%", info.Progress, info.InputLength, info.Progress * 100 / info.InputLength);
            }

            Console.WriteLine("Took {0:n0}ms ({1:#.0} minutes), ( {2}% spent in azure table calls)",
                info.TimeInRead, 
                TimeSpan.FromMilliseconds(info.TimeInRead).TotalMinutes, 
                (info.TimeInRead == 0) ? 0 : info.TimeInUpload * 100 / info.TimeInRead);
            Console.WriteLine("{0:n0} rows uploaded", info.TotalUploaded, info.InputLength);
            if (info.TotalSkipped > 0)
            {
                Console.WriteLine("  ({0:n0} rows skipped due to containing illegal characters \\/#?", info.TotalSkipped);
            }
            if (info.SessionCounter > 1)
            {
                Console.WriteLine("Upload was resumed: {0} sessions", info.SessionCounter);
            }
            return;           
            
        }
        
        readonly static char[] illegalTableChars = new char[] { '\\', '/', '?', '#' };

        internal static bool HasIllegalChars(string value)
        {
            // Part and Row Key can't contain \/?#
            return (value.IndexOfAny(illegalTableChars) > 0);
        }
    }
}
