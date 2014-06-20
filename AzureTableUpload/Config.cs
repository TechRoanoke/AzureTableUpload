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
    // Configuration, mainly for connecting to the server. 
    class Config
    {
        // Provide a convenient cache of connection strings so that input can just use a account name.
        public string[] Credentials { get; set; }

        public class ServerInfo
        {
            public string Account { get; set; }
            public string QueueName { get; set; }
        }
        public ServerInfo Server { get; set; }

        public static Config Load(string filename)
        {
            if (filename == null || !File.Exists(filename))
            {
                return null;
            }
            string json = File.ReadAllText(filename);

            var config = JsonConvert.DeserializeObject<Config>(json);
            config.Map();
            return config;
        }

        public CloudQueue GetQueue()
        {
            string acs = this.Server.Account;
            acs = ResolveAccountString(acs);
            CloudStorageAccount account = CloudStorageAccount.Parse(acs);
            CloudQueueClient client = account.CreateCloudQueueClient();
            CloudQueue queue = client.GetQueueReference(this.Server.QueueName);
            queue.CreateIfNotExists();

            return queue;
        }

        // Private map of AccountName-->AccountConnectionString
        internal IDictionary<string, string> _d;

        void Map()
        {
            _d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var val in this.Credentials)
            {
                CloudStorageAccount acs = CloudStorageAccount.Parse(val);
                string accountName = acs.Credentials.AccountName;
                _d[accountName] = val;
            }
        }
        

        // Resolve an account string. 
        // If it's a full account string, return as-is.
        // If it's just the account name, then lookup in the credential store and return
        public string ResolveAccountString(string x)
        {
            if (x == null)
            {
                return null;
            }
            
            string acs;
            if (_d.TryGetValue(x, out acs))
            {
                return acs;
            }
            return x;
        }
    }
}