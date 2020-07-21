# DurableFileProcessing
Using a Blob Trigger to orchestrate a set of actions to be carried out on the uploaded file

# High-level Description
The workflow is triggered by a Blob being added to the `original-store` container (in the `FileProcessingStorage` Azure Storage Account). When the processing of the file is complete, a message is submitted to the `TransactionOutcomeQueue`.

# Storage
All workflow storage is persisted in `FileProcessingStorage`
`original-store` : A Blob container into which any file to be process is written. The addition of blobs to this store triggers the workflow.
`rebuild-store` : A blob container into which the rebuild version of the file is written. The original file's hash value is used to name the file written to this store.

# Configuration
The following configuration is required in `local.settings.json` for the 'DurableFileProcessing' project folder.
`AzureWebJobsStorage` : The connection string of the Azure Storage Account being used by the framework. For local development use "UseDevelopmentStorage=true". When deployed to Azure, this may use the same storage account as 'FileProcessingStorage'.
`FileProcessingStorage` : The connection string of the Azure Storage Account being used by the workflow logic. In order that the Fileype Detection and Rebuild APIs can access the necessary stores, this Storage Account must be provided within Microsoft Azure.
`ServiceBusConnectionString` : The connection string of the Service Bus Namespace in which the `TransactionOutcomeQueueName` exists. This must have at least a `Send` claim on the namespace.
`TransactionOutcomeQueueName`  : The name of the Storage Queue within `FileProcessingStorage` used to return the processing results.
`FiletypeDetectionUrl` & `FiletypeDetectionKey` : The URL used to access the Filetype Detection API, and its associated key.
'RebuildUrl' & 'RebuildKey' : The URL used to access the Rebuild API, and its associated key.

# Storage Emulator
The [Azure Storage Emulator](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator) can be used to support development by hosting the durable function framework files. Since the file processing APIs need access to the `original` and `rebuilt` stores, these cannot be emulated locally.