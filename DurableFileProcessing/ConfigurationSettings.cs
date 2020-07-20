namespace DurableFileProcessing
{
    public class ConfigurationSettings
    {
        public string FileProcessingStorage { get; set; }
        
        public string ServiceBusConnectionString { get; set; }
        public string TransactionOutcomeQueueName  { get; set; }
    }
}
