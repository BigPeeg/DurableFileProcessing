using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;

namespace DurableFileProcessing
{
    public static class FileProcessing
    {
        [FunctionName("FileProcessing")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            [Blob("original-store")] CloudBlobContainer container,
            ILogger log)
        {
            var transactionId = context.NewGuid().ToString();
            var blobName = context.GetInput<string>();

            string blobSas = BlobUtilities.GetSharedAccessSignature(container, blobName, context.CurrentUtcDateTime.AddHours(24), SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write);
            var configurationSettings = await context.CallActivityAsync<ConfigurationSettings>("FileProcessing_GetConfigurationSettings", null);

            log.LogInformation($"FileProcessing SAS Token: {blobSas}");

            var hash = await context.CallActivityAsync<string>("FileProcessing_HashGenerator", blobSas);

            var filetype = await context.CallActivityAsync<string>("FileProcessing_GetFileType", blobSas);

            if (filetype == "unmanaged")
            {
                await context.CallActivityAsync("FileProcessing_SignalTransactionOutcome", (configurationSettings, transactionId, new RebuildOutcome { Outcome = ProcessingOutcome.Unknown, RebuiltFileSas = String.Empty}));
            }
            else
            {
                log.LogInformation($"FileProcessing {filetype}");

                
                var rebuildContainer = new CloudBlobContainer(
                    new Uri(configurationSettings.RebuildStoreLocaton), 
                    new StorageCredentials(configurationSettings.StorageAccount, configurationSettings.StorageAccountKey));
                var sourceSas = BlobUtilities.GetSharedAccessSignature(container, blobName, context.CurrentUtcDateTime.AddHours(24), SharedAccessBlobPermissions.Read);

                // Specify the hash value as the rebuilt filename
                var rebuiltWritesSas = BlobUtilities.GetSharedAccessSignature(rebuildContainer, hash, context.CurrentUtcDateTime.AddHours(24), SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write);
                var rebuildOutcome = await context.CallActivityAsync<ProcessingOutcome>("FileProcessing_RebuildFile", (sourceSas, rebuiltWritesSas, filetype));

                if (rebuildOutcome == ProcessingOutcome.Rebuilt)
                {
                    var rebuiltReadSas = BlobUtilities.GetSharedAccessSignature(rebuildContainer, hash, context.CurrentUtcDateTime.AddHours(24), SharedAccessBlobPermissions.Read);
                    log.LogInformation($"FileProcessing Rebuild {rebuiltReadSas}");

                    await context.CallActivityAsync("FileProcessing_SignalTransactionOutcome", (configurationSettings, transactionId, new RebuildOutcome { Outcome = ProcessingOutcome.Rebuilt, RebuiltFileSas = rebuiltReadSas }));
                }
                else
                {
                    log.LogInformation($"FileProcessing Rebuild failure");
                    await context.CallActivityAsync("FileProcessing_SignalTransactionOutcome", (configurationSettings, transactionId, new RebuildOutcome { Outcome = ProcessingOutcome.Failed, RebuiltFileSas = String.Empty }));
                }
            }
        }

        [FunctionName("FileProcessing_GetConfigurationSettings")]
        public static Task<ConfigurationSettings> GetConfigurationSettings([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            var configurationSettings = new ConfigurationSettings
            {
                StorageAccount = Environment.GetEnvironmentVariable("StorageAccount", EnvironmentVariableTarget.Process),
                StorageAccountKey = Environment.GetEnvironmentVariable("StorageAccountKey", EnvironmentVariableTarget.Process),
                RebuildStoreLocaton = Environment.GetEnvironmentVariable("RebuildStoreLocaton", EnvironmentVariableTarget.Process),
                ServiceBusConnectionString = Environment.GetEnvironmentVariable("ServiceBusConnectionString", EnvironmentVariableTarget.Process),
                TransactionOutcomeQueueName = Environment.GetEnvironmentVariable("TransactionOutcomeQueueName", EnvironmentVariableTarget.Process),
            };

            return Task.FromResult(configurationSettings);
        }

        [FunctionName("FileProcessing_HashGenerator")]
        public static async Task<string> HashGeneratorAsync([ActivityTrigger] string blobSas, ILogger log)
        {
            log.LogInformation($"HashGenerator {blobSas}");
            var rxBlockBlob = new CloudBlockBlob(new Uri(blobSas));

            using (var fileStream = new MemoryStream())
            using (var md5 = MD5.Create())
            {
                await rxBlockBlob.DownloadToStreamAsync(fileStream);

                var hash = md5.ComputeHash(fileStream);
                var base64String = Convert.ToBase64String(hash);
                return base64String;
            }
        }

        [FunctionName("FileProcessing_StoreHash")]
        public static void StoreHash([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            (string transactionId, string hash) = context.GetInput<(string, string)>();
            log.LogInformation($"StoreHash, transactionId='{transactionId}', hash='{hash}'");
        }

        [FunctionName("FileProcessing_CheckAvailableOutcome")]
        public static ProcessingOutcome CheckAvailableOutcome([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            string hash = context.GetInput<string>();
            var outcome = ProcessingOutcome.Unknown;
            log.LogInformation($"CheckAvailableOutcome, hash='{hash}', Outcome = {outcome}");
            return outcome;
        }

        [FunctionName("FileProcessing_SignalTransactionOutcome")]
        public static void SignalTransactionOutcome([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            (ConfigurationSettings configuration, string transactionId, RebuildOutcome outcome) = context.GetInput<(ConfigurationSettings, string, RebuildOutcome)>();
            log.LogInformation($"SignalTransactionOutcome, transactionId='{transactionId}', outcome='{outcome.Outcome}'");
            CloudStorageAccount account;
            CloudStorageAccount.TryParse(configuration.ServiceBusConnectionString, out account);

            var queueClient = account.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(configuration.TransactionOutcomeQueueName);
            var messageContentJson = JsonConvert.SerializeObject(new
            {
                TransactionId = transactionId,
                Outcome = outcome.Outcome,
                RebuildFileSas = outcome.RebuiltFileSas
            });

            queue.AddMessage(new CloudQueueMessage(messageContentJson));
        }
        
        [FunctionName("FileProcessing_GetFileType")]
        public static string GetFileType([ActivityTrigger] string blobSas, ILogger log)
        {
            log.LogInformation($"GetFileType, blobSas='{blobSas}'");
            return "png";
        }
        
        [FunctionName("FileProcessing_RebuildFile")]
        public static async Task<ProcessingOutcome> RebuildFileAsync([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            (string receivedSas, string rebuildSas, string receivedFiletype) = context.GetInput<(string, string, string)>();
            log.LogInformation($"RebuildFileAsync, receivedSas='{receivedSas}', rebuildSas='{rebuildSas}', receivedFiletype='{receivedFiletype}'");
            // This version of the Activity just copies the incoming file to its rebuilt location
            var rxBlockBlob = new CloudBlockBlob(new Uri(receivedSas));
            var rdBlobClient = new CloudBlockBlob(new Uri(rebuildSas));

            var RebuildFileAsync = await rdBlobClient.StartCopyAsync(rxBlockBlob);
            log.LogInformation($"RebuildFileAsync: '{RebuildFileAsync}");

            return ProcessingOutcome.Rebuilt;
        }

        [FunctionName("FileProcessing_BlobTrigger")]
        public static async Task BlobTrigger(
            [BlobTrigger("original-store/{name}")] CloudBlockBlob myBlob, string name,
            [DurableClient] IDurableOrchestrationClient starter, ILogger log)
        {
            string instanceId = await starter.StartNewAsync("FileProcessing", input:name);

            log.LogInformation($"Started orchestration with ID = '{instanceId}', Blob '{name}'.");
        }
    }
}