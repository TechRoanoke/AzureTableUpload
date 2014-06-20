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
    public enum Mode
    {
        None,

        // Validate the local file, don't do anything. 
        Validate,

        // Upload the file to the server from this local machine. 
        // This will create a ProgressStatus artifact on the server
        UploadLocal,

        // Queue a message so that we upload from the server. This is much faster for large tables. 
        UploadServer,

        // Reset any ProgressStatus information on the server. 
        Reset,

        // Print ProgressStauts information from the server.
        Status
    }

    class CommandArgs
    {
        public Mode Mode;

        // Where to read the WorkItem from 
        public WorkItem Work;


        public static void PrintHelp()
        {
            Console.WriteLine(
                @"Tool for uploading an azure table
TableUpload.exe 
-validate %filename-csv% %partkey% %rowkey%
    locally validate the file without uploading. 

-upload 
    begins an upload. Since no json spec is provided, prompts you to enter it 

-upload -json %filename-jsonspec% 
    begins (or resumes) an upload using the provided json spec. 

-upload -server
    queues a message to do the upload on the server. (Do this for large tables)

-status -json %filename-jsonspec% 
    prints the status for the file upload 

-reset -json %filename-jsonspec% 
    resets progress status state on the server. This can be used to repeat an upload

Other flags:
-config %filename-config% 
    path to a config file container 
    default is '%this executable%\config.txt'
");
        }

        public static CommandArgs Parse(string[] args)
        {
            if (args == null || args.Length == 0)
            {               
                return null;
            }

            string localPath = new Uri(typeof(Program).Assembly.Location).LocalPath;
            string configFilename = Path.Combine(Path.GetDirectoryName(localPath), "config.txt");

            string jsonFilename = null;
            bool forceUpload = false;
            CommandArgs opts = new CommandArgs();


            List<string> other = new List<string>();
            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "-reset":
                        opts.Mode = Mode.Reset;
                        break;
                    case "-validate":
                        opts.Mode = Mode.Validate;
                        break;
                    case "-upload":
                        opts.Mode = Mode.UploadLocal;
                        break;
                    case "-status":
                        opts.Mode = Mode.Status;
                        break;
                    case "-server":
                        opts.Mode = Mode.UploadServer;
                        break;
                    case "-spec":
                        jsonFilename = args[i + 1];
                        i++;
                        break;
                    case "-force":
                        forceUpload = true;
                        break;
                    case "-config":
                        configFilename = args[i + 1];
                        i++;
                        break;

                    default:
                        if (args[i][0] == '-')
                        {
                            Log.WriteLine(ConsoleColor.Red, "Unrecognized switch: {0}", args[i]);
                            return null;
                        }
                        other.Add(args[i]);
                        break;
                }
            }



            switch (opts.Mode)
            {
                case Mode.None:
                    Log.WriteLine(ConsoleColor.Red, "Mode is not specified");
                    return null;

                case Mode.Validate:
                    if (jsonFilename == null)
                    {
                        opts.Work = new WorkItem();
                        opts.Work.InputFilename = other[0];

                        if (other.Count == 1)
                        {
                            InputPartitionRowKey(opts.Work);
                        }
                        else if (other.Count == 3)
                        {
                            opts.Work.PartKeyName = other[1];
                            opts.Work.RowKeyName = other[2];
                        }
                        else
                        {
                            Log.WriteLine(ConsoleColor.Red, "Expected command line to container %filename% %partKey% %rowkey% ");
                            return null;
                        }
                    }
                    break;

                case Mode.Reset:
                case Mode.Status:
                    if (jsonFilename == null)
                    {
                        // This is reuqired. 
                        Log.WriteLine(ConsoleColor.Red, "Error: json filename spec is required for -status and -reset");
                        return null;
                    }
                    break;
            }


            if (configFilename != null)
            {
                if (File.Exists(configFilename))
                {
                    Program._config = Config.Load(configFilename);
                }
                else
                {
                    Log.WriteLine(ConsoleColor.Yellow, "No config file at: {0}", configFilename);
                    Console.WriteLine("You can override this search path with -config");
                }
            }

            if (opts.Work == null)
            {
                if (jsonFilename == null)
                {
                    Log.WriteLine(ConsoleColor.Yellow, "No -spec flag specified, input values now to create a spec...");
                    var work = InputWorkFromConsole();

                    jsonFilename = Path.Combine(Environment.CurrentDirectory, work.TableName + "-spec.txt");

                    // When we serialize, include all fields so it's easy to edit. 
                    string json = JsonConvert.SerializeObject(work, Formatting.Indented);
                    File.WriteAllText(jsonFilename, json);
                    Console.WriteLine("Spec is saved to:");
                    Log.WriteLine(ConsoleColor.White, " {0}", jsonFilename);
                    // Save it to file. 
                }

                opts.Work = JsonConvert.DeserializeObject<WorkItem>(File.ReadAllText(jsonFilename));
            }

            // Resolve after we've deserialized. 
            // This reduces the number of passwords copied on disk.
            if (Program._config != null)
            {
                opts.Work.AccountConnectionString = Program._config.ResolveAccountString(opts.Work.AccountConnectionString);
            }

            return opts;

        }

        static void InputPartitionRowKey(WorkItem work)
        {
            var partRowKey = InputPartitionRowKey(work.InputFilename);
            work.PartKeyName = partRowKey.Item1;
            work.RowKeyName = partRowKey.Item2;
        }

        static Tuple<string, string> InputPartitionRowKey(string filename)
        {
            // Select columns 
            Console.WriteLine("Here are the column names and first row of data:");

            string[] columnNames;
            {
                var dt = DataTable.New.ReadLazy(filename);
                columnNames = dt.ColumnNames.ToArray();

                var row = dt.Rows.FirstOrDefault();
                foreach (var columnName in columnNames)
                {
                    string value = null;
                    if (row != null)
                    {
                        value = row[columnName];
                    }
                    Console.WriteLine("  {0}: {1}", columnName, value);
                }
            }
            Console.WriteLine();

            // $$$ Could make a guess at these...
            Console.WriteLine("Which column is the Partition key? ");
            string partKeyName = Console.ReadLine().Trim();

            Console.WriteLine("Which column is the Row key? ");
            string rowKeyName = Console.ReadLine().Trim();

            return Tuple.Create(partKeyName, rowKeyName);
        }

        static WorkItem InputWorkFromConsole()
        {
            // If it's just an account name, we can query for password and get connection string
            Console.WriteLine("Enter the full account connection string:");

            if (Program._config != null && Program._config.Credentials != null)
            {
                Console.WriteLine("or one the storage account names below:");
                foreach (var x in Program._config._d)
                {
                    Console.WriteLine("  {0}", x.Key);
                }
            }

            string acs = Console.ReadLine().Trim();

            // $$$ Validate the account 

            Console.WriteLine("Enter table name (3-63 chars, case-insensitive, alphanumeric):");
            string tableName = Console.ReadLine().Trim();
        // $$$ Warn if already exists?

        RetryFile:
            Console.WriteLine("Enter local filename:");
            string local = Console.ReadLine().Trim();

            if (!File.Exists(local))
            {
                Console.WriteLine("Error: file does not exist");
                goto RetryFile;
            }

            // Dump column names

            WorkItem work = new WorkItem
            {
                AccountConnectionString = acs,
                TableName = tableName,
                InputFilename = local
            };

            // Select columns 
            InputPartitionRowKey(work);            

            // Blob to upload to 
            Console.WriteLine("We first copy the file to a blob, and then ingress that to a table.");
            string defaultContainerName = "table-uploads";
            Console.WriteLine("Enter container name to upload the csv to (default '{0}')", defaultContainerName);
            string temp = Console.ReadLine().Trim();
            work.InputContainerName = string.IsNullOrWhiteSpace(temp) ? defaultContainerName : temp;

            work.InputBlobName = Path.GetFileName(local);


            return work;
        }
    }
}