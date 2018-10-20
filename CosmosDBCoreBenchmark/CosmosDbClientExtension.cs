namespace CosmosDBCoreBenchmark
{
    using System;
    using Microsoft.Azure.Documents.Client;

    public class CosmosDbClientExtension
    {
        private static DocumentClient DocumentClient {get;set;}


        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp,
            RequestTimeout = new TimeSpan(1, 0, 0),
            MaxConnectionLimit = 1000,
            RetryOptions = new RetryOptions
            {
                MaxRetryAttemptsOnThrottledRequests = 10,
                MaxRetryWaitTimeInSeconds = 60
            }
        };

        public static DocumentClient GetClient()
        {
            if (DocumentClient != null)
            {
                return DocumentClient;
            }
            
            ConfigSettings config=ConfigSettings.Get();
            DocumentClient = new DocumentClient(
                new Uri(config.EndPointUrl),
                config.AuthorizationKey,
                ConnectionPolicy);

            return DocumentClient;
        }

    }
}
