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
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            [Blob("functionstest")] CloudBlobContainer container,
            ILogger log)
        {
            var filename = context.GetInput<string>();

            string containerSas = GetSharedAccessSignature(container, context.CurrentUtcDateTime.AddHours(24));

            log.LogInformation($"FileProcessing SAS Token: {containerSas}");

            var hash = await context.CallActivityAsync<string>("FileProcessing_HashGenerator", (containerSas, filename));

            log.LogInformation($"FileProcessing {container.Uri}, name='{filename}', hash='{hash}'");

            var outputs = new List<string>();

            // Replace "hello" with the name of your Durable Activity Function.
            outputs.Add(await context.CallActivityAsync<string>("FileProcessing_Hello", "Tokyo"));
            outputs.Add(await context.CallActivityAsync<string>("FileProcessing_Hello", "Seattle"));
            outputs.Add(await context.CallActivityAsync<string>("FileProcessing_Hello", "London"));

            // returns ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]
            return outputs;
        }

        private static string GetSharedAccessSignature(CloudBlobContainer container, DateTimeOffset expiryTime)
        {
            SharedAccessBlobPolicy adHocPolicy = new SharedAccessBlobPolicy()
            {
                // When the start time for the SAS is omitted, the start time is assumed to be the time when the storage service receives the request.
                // Omitting the start time for a SAS that is effective immediately helps to avoid clock skew.
                SharedAccessExpiryTime = expiryTime,
                Permissions = SharedAccessBlobPermissions.Read 
            };

            var sasContainerToken = container.GetSharedAccessSignature(adHocPolicy);

            return container.Uri + sasContainerToken;
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

        [FunctionName("FileProcessing_Hello")]
        public static string SayHello([ActivityTrigger] string name, ILogger log)
        {
            log.LogInformation($"Saying hello to {name}.");
            return $"Hello {name}!";
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