using System;
using System.Collections.Generic;
using System.Text;

namespace DurableFileProcessing
{
    public class ConfigurationSettings
    {
        public string StorageAccount { get; set; }
        public string StorageAccountKey { get; set; }
        public string RebuildStoreLocaton {get;set;}
        public string ServiceBusConnectionString { get; set; }
        public string TransactionOutcomeQueueName  { get; set; }
    }
}
