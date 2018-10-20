namespace CosmosDBCoreBenchmark
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json;

    public class PerfRun
    {
        
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
        private static readonly string InstanceId =
            Dns.GetHostEntry("LocalHost").HostName + Process.GetCurrentProcess().Id;

        private static readonly ConfigSettings config= ConfigSettings.Get();

        private const int MinThreadPoolSize = 100;

        private int pendingTaskCount;
        private long documentsInserted;
        private ConcurrentDictionary<int, double> requestUnitsConsumed = new ConcurrentDictionary<int, double>();
        private DocumentClient client;

        /// <summary>
        /// Initializes a new instance of the <see cref="Program"/> class.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        public PerfRun(DocumentClient client)
        {

            this.client = client;
        }

        /// <summary>
        /// Run samples for Order By queries.
        /// </summary>
        /// <returns>a Task object.</returns>
        public async Task RunAsync()
        {

            DocumentCollection dataCollection = GetCollectionIfExists(
                config.DatabaseName, 
                config.CollectionName);
            int currentCollectionThroughput = 0;

            if (config.ShouldCleanupOnStart || dataCollection == null)
            {
                Database database = GetDatabaseIfExists(config.DatabaseName);
                if (database != null)
                {
                    await client.DeleteDatabaseAsync(database.SelfLink);
                }

                Console.WriteLine("Creating database {0}", config.DatabaseName);
                database = await client.CreateDatabaseAsync(new Database { Id = config.DatabaseName });

                Console.WriteLine(
                    "Creating collection {0} with {1} RU/s", 
                    config.CollectionName,
                    config.CollectionThroughput);

                dataCollection = await this.CreatePartitionedCollectionAsync(
                    config.DatabaseName, 
                    config.CollectionName);

                currentCollectionThroughput = config.CollectionThroughput;
            }
            else
            {
                OfferV2 offer = (OfferV2)client.CreateOfferQuery()
                                .Where(o => o.ResourceLink == dataCollection.SelfLink)
                                .AsEnumerable()
                                .FirstOrDefault();

                currentCollectionThroughput = offer.Content.OfferThroughput;

                Console.WriteLine(
                    "Found collection {0} with {1} RU/s", 
                    config.CollectionName, 
                    currentCollectionThroughput);
            }

            int taskCount;
            int degreeOfParallelism = config.DegreeOfParallelism;

            if (degreeOfParallelism == -1)
            {
                // set TaskCount = 10 for each 10k RUs, minimum 1, maximum 250
                taskCount = Math.Max(currentCollectionThroughput / 1000, 1);
                taskCount = Math.Min(taskCount, 250);
            }
            else
            {
                taskCount = degreeOfParallelism;
            }

            Console.WriteLine("Starting Inserts with {0} tasks", taskCount);
            string sampleDocument = File.ReadAllText(config.DocumentTemplateFile);

            pendingTaskCount = taskCount;
            var tasks = new List<Task>();
            tasks.Add(this.LogOutputStats());

            long numberOfDocumentsToInsert = config.NumberOfDocumentsToInsert / taskCount;
            for (var i = 0; i < taskCount; i++)
            {
                tasks.Add(this.InsertDocument(i, client, dataCollection, sampleDocument, numberOfDocumentsToInsert));
            }

            await Task.WhenAll(tasks);

            if (config.ShouldCleanupOnFinish)
            {
                Console.WriteLine("Deleting Database {0}", config.DatabaseName);
                await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(config.DatabaseName));
            }
        }

        private async Task InsertDocument(
            int taskId, 
            DocumentClient client, 
            DocumentCollection collection, 
            string sampleJson, 
            long numberOfDocumentsToInsert)
        {
            requestUnitsConsumed[taskId] = 0;
            string partitionKeyProperty = collection.PartitionKey.Paths[0].Replace("/", "");
            Dictionary<string, object> newDictionary = JsonConvert.DeserializeObject<Dictionary<string, object>>(sampleJson);

            for (var i = 0; i < numberOfDocumentsToInsert; i++)
            {
                newDictionary["id"] = Guid.NewGuid().ToString();
                newDictionary[partitionKeyProperty] = Guid.NewGuid().ToString();

                try
                {
                    ResourceResponse<Document> response = await client.CreateDocumentAsync(
                            UriFactory.CreateDocumentCollectionUri(config.DatabaseName, config.CollectionName),
                            newDictionary,
                            new RequestOptions() { });

                    string partition = response.SessionToken.Split(':')[0];
                    requestUnitsConsumed[taskId] += response.RequestCharge;
                    Interlocked.Increment(ref this.documentsInserted);
                }
                catch (Exception e)
                {
                    if (e is DocumentClientException)
                    {
                        DocumentClientException de = (DocumentClientException)e;
                        if (de.StatusCode != HttpStatusCode.Forbidden)
                        {
                            Trace.TraceError(
                                "Failed to write {0}. Exception was {1}", 
                                JsonConvert.SerializeObject(newDictionary),
                                e);
                        }
                        else
                        {
                            Interlocked.Increment(ref this.documentsInserted);
                        }
                    }
                }
            }

            Interlocked.Decrement(ref this.pendingTaskCount);
        }

        private async Task LogOutputStats()
        {
            long lastCount = 0;
            double lastRequestUnits = 0;
            double lastSeconds = 0;
            double requestUnits = 0;
            double ruPerSecond = 0;
            double ruPerMonth = 0;

            Stopwatch watch = new Stopwatch();
            watch.Start();

            while (this.pendingTaskCount > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                double seconds = watch.Elapsed.TotalSeconds;

                requestUnits = 0;
                foreach (int taskId in requestUnitsConsumed.Keys)
                {
                    requestUnits += requestUnitsConsumed[taskId];
                }

                long currentCount = this.documentsInserted;
                ruPerSecond = (requestUnits / seconds);
                ruPerMonth = ruPerSecond * 86400 * 30;

                Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                    currentCount,
                    Math.Round(this.documentsInserted / seconds),
                    Math.Round(ruPerSecond),
                    Math.Round(ruPerMonth / (1000 * 1000 * 1000)));

                lastCount = documentsInserted;
                lastSeconds = seconds;
                lastRequestUnits = requestUnits;
            }

            double totalSeconds = watch.Elapsed.TotalSeconds;
            ruPerSecond = (requestUnits / totalSeconds);
            ruPerMonth = ruPerSecond * 86400 * 30;

            Console.WriteLine();
            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("Inserted {0} docs @ {1} writes/s, {2} RU/s ({3}B max monthly 1KB reads)",
                lastCount,
                Math.Round(this.documentsInserted / watch.Elapsed.TotalSeconds),
                Math.Round(ruPerSecond),
                Math.Round(ruPerMonth / (1000 * 1000 * 1000)));
            Console.WriteLine("--------------------------------------------------------------------- ");
        }

        /// <summary>
        /// Create a partitioned collection.
        /// </summary>
        /// <returns>The created collection.</returns>
        private async Task<DocumentCollection> CreatePartitionedCollectionAsync(string databaseName, string collectionName)
        {
            DocumentCollection existingCollection = GetCollectionIfExists(databaseName, collectionName);

            DocumentCollection collection = new DocumentCollection();
            collection.Id = collectionName;
            collection.PartitionKey.Paths.Add(config.CollectionPartitionKey);

            // Show user cost of running this test
            double estimatedCostPerMonth = 0.06 * config.CollectionThroughput;
            double estimatedCostPerHour = estimatedCostPerMonth / (24 * 30);
            Console.WriteLine("The collection will cost an estimated ${0} per hour (${1} per month)", Math.Round(estimatedCostPerHour, 2), Math.Round(estimatedCostPerMonth, 2));
            Console.WriteLine("Press enter to continue ...");
            Console.ReadLine();

            return await client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    collection,
                    new RequestOptions { OfferThroughput = config.CollectionThroughput });
        }

        /// <summary>
        /// Get the database if it exists, null if it doesn't
        /// </summary>
        /// <returns>The requested database</returns>
        private Database GetDatabaseIfExists(string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Get the collection if it exists, null if it doesn't
        /// </summary>
        /// <returns>The requested collection</returns>
        private DocumentCollection GetCollectionIfExists(string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(databaseName) == null)
            {
                return null;
            }

            return client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

    }
}
