using Dynamitey.DynamicObjects;
using Flurl;
using Flurl.Http;
using Microsoft.Azure.Storage;
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
    [StorageAccount("FileProcessingStorage")]
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

            var filetype = await context.CallActivityAsync<string>("FileProcessing_GetFileType", (configurationSettings, blobSas));

            if (filetype == "unmanaged")
            {
                await context.CallActivityAsync("FileProcessing_SignalTransactionOutcome", (configurationSettings, transactionId, new RebuildOutcome { Outcome = ProcessingOutcome.Unknown, RebuiltFileSas = String.Empty}));
            }
            else
            {
                log.LogInformation($"FileProcessing {filetype}");
                var fileProcessingStorage = CloudStorageAccount.Parse(configurationSettings.FileProcessingStorage);
                var rebuildUrl = Url.Combine(fileProcessingStorage.BlobEndpoint.AbsoluteUri, "rebuild-store");
                log.LogInformation($"FileProcessing using  {rebuildUrl}");

                var rebuildContainer = new CloudBlobContainer(new Uri(rebuildUrl), fileProcessingStorage.Credentials);
                var sourceSas = BlobUtilities.GetSharedAccessSignature(container, blobName, context.CurrentUtcDateTime.AddHours(24), SharedAccessBlobPermissions.Read);

                // Specify the hash value as the rebuilt filename
                var rebuiltWritesSas = BlobUtilities.GetSharedAccessSignature(rebuildContainer, hash, context.CurrentUtcDateTime.AddHours(24), SharedAccessBlobPermissions.Write);
                var rebuildOutcome = await context.CallActivityAsync<ProcessingOutcome>("FileProcessing_RebuildFile", (configurationSettings, sourceSas, rebuiltWritesSas, filetype));

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
        public static Task<ConfigurationSettings> GetConfigurationSettings([ActivityTrigger] IDurableActivityContext context)
        {
            var configurationSettings = new ConfigurationSettings
            {
                FileProcessingStorage = Environment.GetEnvironmentVariable("FileProcessingStorage", EnvironmentVariableTarget.Process),
                TransactionOutcomeQueueName = Environment.GetEnvironmentVariable("TransactionOutcomeQueueName", EnvironmentVariableTarget.Process),
                FiletypeDetectionUrl = Environment.GetEnvironmentVariable("FiletypeDetectionUrl", EnvironmentVariableTarget.Process),
                FiletypeDetectionKey = Environment.GetEnvironmentVariable("FiletypeDetectionKey", EnvironmentVariableTarget.Process),
                RebuildUrl = Environment.GetEnvironmentVariable("RebuildUrl", EnvironmentVariableTarget.Process),
                RebuildKey = Environment.GetEnvironmentVariable("RebuildKey", EnvironmentVariableTarget.Process),
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
            var fileProcessingStorage = CloudStorageAccount.Parse(configuration.FileProcessingStorage);

            var queueClient = fileProcessingStorage.CreateCloudQueueClient();
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
        public static async Task<string> GetFileTypeAsync([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            (ConfigurationSettings configuration, string blobSas) = context.GetInput<(ConfigurationSettings, string)>();
            var filetypeDetectionUrl = configuration.FiletypeDetectionUrl;
            var filetypeDetectionKey = configuration.FiletypeDetectionKey;

            log.LogInformation($"GetFileType, filetypeDetectionUrl='{filetypeDetectionUrl}'");
            log.LogInformation($"GetFileType, blobSas='{blobSas}'");
            var response = await filetypeDetectionUrl
                .WithHeader("x-api-key", filetypeDetectionKey)
                .PostJsonAsync(new
                {
                    SasUrl = blobSas
                })
                .ReceiveJson();

            return response.FileTypeName;
        }
        
        [FunctionName("FileProcessing_RebuildFile")]
        public static async Task<ProcessingOutcome> RebuildFileAsync([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            (ConfigurationSettings configuration, string receivedSas, string rebuildSas, string receivedFiletype) = context.GetInput<(ConfigurationSettings, string, string, string)>();
            log.LogInformation($"RebuildFileAsync, receivedSas='{receivedSas}', rebuildSas='{rebuildSas}', receivedFiletype='{receivedFiletype}'");
            var rebuildUrl = configuration.RebuildUrl;
            var rebuildKey = configuration.RebuildKey;
            var response = await rebuildUrl
                .SetQueryParam("code", rebuildKey, isEncoded:true)
                .PostJsonAsync(new
                {
                    InputGetUrl = receivedSas,
                    OutputPutUrl = rebuildSas,
                    OutputPutUrlRequestHeaders = new Dictionary
                    {
                         { "x-ms-blob-type", "BlockBlob" }
                    }
                })
                .ReceiveString();
            log.LogInformation($"GetFileType, response='{response}'");

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