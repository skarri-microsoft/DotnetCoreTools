namespace CosmosDBCoreBenchmark
{
    using System;
    using Microsoft.Azure.Documents.Client;

    class Program
    {
        /// <summary>
        /// The DocumentDB client instance.
        /// </summary>
        private DocumentClient client;
        static void Main(string[] args)
        {

            ConfigSettings configSettings = ConfigSettings.Get();
            configSettings.Print();

            Console.WriteLine("CosmosDBSqlCoreBenchmark starting...");
            try
            {
                DocumentClient documentClient = CosmosDbClientExtension.GetClient();

                PerfRun perfRun = new PerfRun(documentClient);

                perfRun.RunAsync().Wait();

                Console.WriteLine("DocumentDBBenchmark completed successfully.");
            }
            catch (Exception e)
            {
                // If the Exception is a DocumentClientException, the "StatusCode" value might help identity 
                // the source of the problem. 
                Console.WriteLine("Samples failed with exception:{0}", e);
            }
            finally
            {
                Console.WriteLine("Press any key to exit...");
                Console.ReadLine();
            }

        }


    }
}