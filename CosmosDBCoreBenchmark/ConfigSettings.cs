namespace CosmosDBCoreBenchmark
{
    using System;
    using Microsoft.Extensions.Configuration;

    public class ConfigSettings
    {
        public string EndPointUrl { get; set; }
        public string AuthorizationKey { get; set; }
        public string DatabaseName { get; set; }

        public string CollectionName { get; set; }

        public int CollectionThroughput { get; set; }

        public bool ShouldCleanupOnStart { get; set; }

        public int DegreeOfParallelism { get; set; }

        public bool ShouldCleanupOnFinish { get; set; }

        public string DocumentTemplateFile { get; set; }

        public string CollectionPartitionKey { get; set; }

        public int NumberOfDocumentsToInsert { get; set; }

        public void Print()
        {
            Console.WriteLine("Config Settings:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("Endpoint: {0}", EndPointUrl);
            Console.WriteLine("Collection : {0}.{1} at {2} request units per second", 
                DatabaseName, CollectionName, CollectionThroughput);
            Console.WriteLine("Document Template*: {0}", DocumentTemplateFile);
            Console.WriteLine("Degree of parallelism*: {0}", DegreeOfParallelism);
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine();
        }

        public static ConfigSettings Get()
        {
            ConfigSettings config=new ConfigSettings();

            IConfigurationRoot configuration = 
                new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();
            config.EndPointUrl = configuration["EndPointUrl"];
            config.AuthorizationKey= configuration["AuthorizationKey"];
            config.DatabaseName= configuration["DatabaseName"];
            config.CollectionName= configuration["CollectionName"];

            config.CollectionThroughput=Int32.Parse(configuration["CollectionThroughput"]);
            config.CollectionPartitionKey= configuration["CollectionPartitionKey"];
            config.DegreeOfParallelism = Int32.Parse(configuration["DegreeOfParallelism"]);
            config.DocumentTemplateFile = configuration["DocumentTemplateFile"];
            config.NumberOfDocumentsToInsert= Int32.Parse(configuration["NumberOfDocumentsToInsert"]);

            config.ShouldCleanupOnStart= bool.Parse(configuration["ShouldCleanupOnStart"]);
            config.ShouldCleanupOnFinish = bool.Parse(configuration["ShouldCleanupOnFinish"]);

            return config;

        }
    }
}
