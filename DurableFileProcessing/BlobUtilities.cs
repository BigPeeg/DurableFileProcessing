using System;
using Microsoft.Azure.Storage.Blob;

namespace DurableFileProcessing
{
    public static class BlobUtilities
    {
        public static string GetSharedAccessSignature(CloudBlobContainer container, DateTimeOffset expiryTime)
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
    }
}
