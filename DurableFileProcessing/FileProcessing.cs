using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace DurableFileProcessing
{
    public static class FileProcessing
    {
        [FunctionName("FileProcessing")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            [Blob("functionstest")] CloudBlobContainer container,
            ILogger log)
        {
            var transactionId = context.NewGuid().ToString();
            var filename = context.GetInput<string>();

            string containerSas = BlobUtilities.GetSharedAccessSignature(container, context.CurrentUtcDateTime.AddHours(24));

            log.LogInformation($"FileProcessing SAS Token: {containerSas}");

            var hash = await context.CallActivityAsync<string>("FileProcessing_HashGenerator", (containerSas, filename));
            await context.CallActivityAsync("FileProcessing_StoreHash", (transactionId, hash));

            var filetype = await context.CallActivityAsync<string>("FileProcessing_GetFileType", (containerSas, filename));

            if (filetype == "unmanaged")
            {
                await context.CallActivityAsync("FileProcessing_SignalTransactionOutcome", (transactionId, ProcessingOutcome.Unknown));
            }
            else
            {
                var rebuildOutcome = await context.CallActivityAsync<ProcessingOutcome>("FileProcessing_RebuildFile", (containerSas, hash, filetype));

                if (rebuildOutcome == ProcessingOutcome.Rebuilt)
                {
                    await context.CallActivityAsync("FileProcessing_SignalTransactionOutcome", (transactionId, rebuildOutcome));
                }
                else
                {
                    await context.CallActivityAsync("FileProcessing_SignalTransactionOutcome", (transactionId, rebuildOutcome));
                }
            }
        }

        [FunctionName("FileProcessing_HashGenerator")]
        public static async Task<string> HashGeneratorAsync([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            (string containerSas, string filename) = context.GetInput<(string, string)>();

            log.LogInformation($"HashGenerator {containerSas}");
            var cloudBlobContainer = new CloudBlobContainer(new Uri(containerSas));
            var blobReference = cloudBlobContainer.GetBlockBlobReference(filename);

            using (var fileStream = new MemoryStream())
            using (var md5 = MD5.Create())
            {
                await blobReference.DownloadToStreamAsync(fileStream, CancellationToken.None);

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
            (string transactionId, ProcessingOutcome outcome) = context.GetInput<(string, ProcessingOutcome)>();
            log.LogInformation($"SignalTransactionOutcome, transactionId='{transactionId}', outcome='{outcome}'");
        }
        
        [FunctionName("FileProcessing_GetFileType")]
        public static string GetFileType([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            (string containerSas, string filename) = context.GetInput<(string, string)>();
            log.LogInformation($"GetFileType, containerSas='{containerSas}', filename='{filename}'");
            return "unknown";
        }

        
        [FunctionName("FileProcessing_RebuildFile")]
        public static ProcessingOutcome RebuildFile([ActivityTrigger] IDurableActivityContext context, ILogger log)
        {
            (string rebuiltContainerSas, string filename, string filetype) = context.GetInput<(string, string, string)>();
            log.LogInformation($"GetFileType, containerSas='{rebuiltContainerSas}', filename='{filename}', filetype='{filetype}'");
            return ProcessingOutcome.Failed; // When we rebuild the new content will be referenced by supplied SAS
        }


        [FunctionName("FileProcessing_BlobTrigger")]
        public static async Task BlobTrigger(
            [BlobTrigger("functionstest/{name}")] CloudBlockBlob myBlob, string name,
            [DurableClient] IDurableOrchestrationClient starter, ILogger log)
        {
            string instanceId = await starter.StartNewAsync("FileProcessing", input:name);

            log.LogInformation($"Started orchestration with ID = '{instanceId}', Blob '{name}'.");
        }
    }
}