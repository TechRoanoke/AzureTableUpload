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
    public class Ingress
    {
        // collect by partition key 
        static Dictionary<string, TableBatchOperation> _batches = new Dictionary<string, TableBatchOperation>();

        static string[] _columnNames;
        static int _rowKeyIdx;
        static int _partKeyIdx;

        public static void Init(WorkItem work, DataTable dt)
        {
            Init(work, dt.ColumnNames.ToArray());
        }

        public static void Init(WorkItem work, string[] columnNames)
        {
            _columnNames = columnNames;
            _partKeyIdx = GetIndex(columnNames, work.PartKeyName);
            _rowKeyIdx = GetIndex(columnNames, work.RowKeyName);
        }

        static int GetIndex(string[] names, string value)
        {
            for (int i = 0; i < names.Length; i++)
            {
                if (string.Equals(names[i], value, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
            throw new InvalidOperationException("Can't find column named '" + value + "'.");
        }

        public static string GetRowKey(Row row)
        {
            return row.Values[_rowKeyIdx];
        }

        public static string GetPartitionKey(Row row)
        {
            return row.Values[_partKeyIdx];
        }

        private static DynamicTableEntity FromRow(Row row)
        {
            var d = new DynamicTableEntity(GetPartitionKey(row), GetRowKey(row));

            for (int i = 0; i < _columnNames.Length; i++)
            {
                d[_columnNames[i]] = new EntityProperty(row.Values[i]);
            }
            return d;
        }

        // Call the table APIs to upload the table.
        // This could be called either locally (slow) or from the cloud (fast)
        // If work.FileName != null, then upload from a local store. Else upload from the blob. 
        // When uploading, avoid the append-only pattern and instead send them in random order to cooperate with load balacing. 
        // http://blogs.msdn.com/b/windowsazurestorage/archive/2010/11/06/how-to-get-most-out-of-windows-azure-tables.aspx 
        public static void Upload(WorkItem work)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(work.AccountConnectionString);
            Console.WriteLine(@"Begin uploading Table to {0}\{1}", account.Credentials.AccountName, work.TableName);

            string etag = work.GetETag();

            // If it already exists, and not completed, then this is a resume operation 
            int skipUpload = 0;

            ProgressStatus info = work.GetProgressStatus();
            if (info != null)
            {
                // Do etags match?
                if (etag == info.InputBlobETag)
                {
                    if (info.Completed)
                    {
                        // Already uploaded this etag.
                        Log.WriteLine(ConsoleColor.Yellow, "Table has already been uploaded. Skipping upload");
                        Console.WriteLine("Delete the progress-status blob to reset this. ({0})", work.GetProgressStatusBlob().Uri);
                        return;
                    }
                    info.StartRow = info.TotalUploaded;
                    skipUpload = info.StartRow;
                    Log.WriteLine(ConsoleColor.Yellow, "Table is in middle of upload. Resuming at row {0}", info.StartRow);
                }
            }
            else
            {
                info = new ProgressStatus();
            }
            info.InputLength = work.GetInputLength();
            info.SessionCounter++;
            info.InputBlobETag = etag;
            info.CountUploadedThisSession = 0;

            CloudTable table = account.CreateCloudTableClient().GetTableReference(work.TableName);
            table.CreateIfNotExists();


            Stopwatch swAzureWriteTime = new Stopwatch(); // time in azure upload
            Stopwatch swTotalTime = Stopwatch.StartNew(); // total time

            using (Stream stream = work.OpenStream())
            {
                var dt = DataTable.New.ReadLazy(stream);

                //var dt = DataTable.New.ReadLazy(file);
                Init(work, dt);

                int countRowsRead = 0;

                int rowCountUploaded = 0;

                foreach (var row in dt.Rows)
                {
                    info.Progress = stream.Position;

                    DynamicTableEntity d = FromRow(row);
                    string partKey = d.PartitionKey;

                    if (Program.HasIllegalChars(partKey) || Program.HasIllegalChars(d.RowKey))
                    {
                        info.TotalSkipped++;
                        continue; // $$$ better error?
                    }

                    TableBatchOperation batch;
                    if (!_batches.TryGetValue(partKey, out batch))
                    {
                        batch = new TableBatchOperation();
                        _batches[partKey] = batch;
                    }

                    batch.Add(TableOperation.InsertOrReplace(d));
                    countRowsRead++;

                    if (batch.Count > 50)
                    {
                        _batches.Remove(partKey); // reset


                        if (rowCountUploaded >= skipUpload)
                        {
                            // Long running operation, so if we're resuming, then skip these
                            swAzureWriteTime.Start();
                            table.ExecuteBatch(batch);
                            swAzureWriteTime.Stop();
                            info.CountUploadedThisSession += batch.Count;
                            info.TotalUploaded += batch.Count;
                        }

                        rowCountUploaded += batch.Count;

                    }

                    if (countRowsRead % 5000 == 0)
                    {
                        info.TimeInRead = swTotalTime.ElapsedMilliseconds;
                        info.TimeInUpload = swAzureWriteTime.ElapsedMilliseconds;
                        info.TotalRead = countRowsRead;

                        work.UpdateProgressStatus(info);
                    }

                    if (countRowsRead % 100000 == 0)
                    {
                        Console.Write(".");
                    }
                }

                info.TotalRead = countRowsRead;
            } // dispose read stream

            // flush remaining 
            foreach (var batch in _batches.Values)
            {
                table.ExecuteBatch(batch);
                info.TotalUploaded += batch.Count;
                info.CountUploadedThisSession += batch.Count;
            }

            // Update final stats
            info.TimeInRead = swTotalTime.ElapsedMilliseconds; ;
            info.TimeInUpload = swAzureWriteTime.ElapsedMilliseconds;

            swTotalTime.Stop();

            // Mark done
            {
                info.Completed = true;
                work.UpdateProgressStatus(info);
            }
        }
    }
}