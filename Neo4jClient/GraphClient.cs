﻿using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Neo4j.Driver;
using Neo4jClient.ApiModels;
using Neo4jClient.ApiModels.Cypher;
using Neo4jClient.Cypher;
using Neo4jClient.Execution;
using Neo4jClient.Serialization;
using Neo4jClient.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Neo4jClient
{
    public class GraphClient : IRawGraphClient, IInternalTransactionalGraphClient<HttpResponseMessage>, IDisposable
    {
        public ITransactionalGraphClient Tx => this;

        internal const string GremlinPluginUnavailable =
            "You're attempting to execute a Gremlin query, however the server instance you are connected to does not have the Gremlin plugin loaded. If you've recently upgraded to Neo4j 2.0, you'll need to be aware that Gremlin no longer ships as part of the normal Neo4j distribution.  Please move to equivalent (but much more powerful and readable!) Cypher.";
        internal const string MaxExecutionTimeHeaderKey = "max-execution-time";

        public static readonly JsonConverter[] DefaultJsonConverters =
        {
            new TypeConverterBasedJsonConverter(),
            new NullableEnumValueConverter(),
            new TimeZoneInfoConverter(),
            new EnumValueConverter()
        };

        public static readonly DefaultContractResolver DefaultJsonContractResolver = new Neo4jContractResolver();

        private ITransactionManager<HttpResponseMessage> transactionManager;
        private readonly IExecutionPolicyFactory policyFactory;

        public ExecutionConfiguration ExecutionConfiguration { get; private set; }

        internal readonly Uri RootUri;
        internal RootApiResponse RootApiResponse;
        private string defaultDatabase = "neo4j";


        public bool UseJsonStreamingIfAvailable { get; set; }

        public GraphClient(string rootUri, string username = null, string password = null)
            : this(new Uri(rootUri), new HttpClientWrapper(username, password))
        {
        }

        public GraphClient(Uri rootUri, string username = null, string password = null)
            : this(rootUri, new HttpClientWrapper(username, password))
        {
        }

        public virtual async Task ConnectAsync(NeoServerConfiguration configuration = null)
        {
            if (IsConnected)
            {
                return;
            }

            var stopwatch = Stopwatch.StartNew();
            var operationCompletedArgs = new OperationCompletedEventArgs
            {
                QueryText = "Connect",
                ResourcesReturned = 0
            };

            void StopTimerAndNotifyCompleted()
            {
                stopwatch.Stop();
                operationCompletedArgs.TimeTaken = stopwatch.Elapsed;
                OnOperationCompleted(operationCompletedArgs);
            }

            try
            {
                configuration = configuration ?? await NeoServerConfiguration.GetConfigurationAsync(
                                    RootUri,
                                    ExecutionConfiguration.Username,
                                    ExecutionConfiguration.Password,
                                    ExecutionConfiguration.Realm,
                                    ExecutionConfiguration.EncryptionLevel,
                                    ExecutionConfiguration).ConfigureAwait(false);

                RootApiResponse = configuration.ApiConfig;

                if (!string.IsNullOrWhiteSpace(RootApiResponse.Transaction))
                {
                    transactionManager = new TransactionManager(this);
                }


                // http://blog.neo4j.org/2012/04/streaming-rest-api-interview-with.html
                ExecutionConfiguration.UseJsonStreaming = ExecutionConfiguration.UseJsonStreaming &&
                                                          RootApiResponse.Version >= new Version(1, 8);

                var version = RootApiResponse.Version;
                if (version < new Version(2, 0))
                    CypherCapabilities = CypherCapabilities.Cypher19;

                if (version >= new Version(2, 2))
                    CypherCapabilities = CypherCapabilities.Cypher22;

                if (version >= new Version(2, 2, 6))
                    CypherCapabilities = CypherCapabilities.Cypher226;

                if (version >= new Version(2, 3))
                    CypherCapabilities = CypherCapabilities.Cypher23;

                if (version >= new Version(3, 0))
                    CypherCapabilities = CypherCapabilities.Cypher30;

                if (version >= new Version(3, 5))
                    CypherCapabilities = CypherCapabilities.Cypher35;

                if (version >= new Version(4, 0))
                    CypherCapabilities = CypherCapabilities.Cypher40;
                if(ServerVersion >= new Version(4,4))
                    CypherCapabilities = CypherCapabilities.Cypher44;
            }
            catch (AggregateException ex)
            {
                var wasUnwrapped = ex.TryUnwrap(out var unwrappedException);
                operationCompletedArgs.Exception = wasUnwrapped ? unwrappedException : ex;

                StopTimerAndNotifyCompleted();

                if (wasUnwrapped)
                    throw unwrappedException;

                throw;
            }
            catch (Exception e)
            {
                operationCompletedArgs.Exception = e;
                StopTimerAndNotifyCompleted();
                throw;
            }

            StopTimerAndNotifyCompleted();
        }

        public GraphClient(Uri rootUri, IHttpClient httpClient)
        {
            RootUri = rootUri;
            JsonConverters = new List<JsonConverter>();
            JsonConverters.AddRange(DefaultJsonConverters);
            JsonContractResolver = DefaultJsonContractResolver;
            ExecutionConfiguration = new ExecutionConfiguration
            {
                HttpClient = httpClient,
                UserAgent = $"Neo4jClient/{GetType().GetTypeInfo().Assembly.GetName().Version}",
                UseJsonStreaming = true,
                JsonConverters = JsonConverters,
                Username = httpClient?.Username,
                Password = httpClient?.Password
            };
            UseJsonStreamingIfAvailable = true;
            policyFactory = new ExecutionPolicyFactory(this);
        }


        // This is where the issue comes in - the 'Cypher' endpoint doesn't exist on a 4.x db - so 
        //
        private Uri BuildUri(string relativeUri)
        {
            var baseUri = RootUri;
            if (!RootUri.AbsoluteUri.EndsWith("/"))
                baseUri = new Uri(RootUri.AbsoluteUri + "/");

            if (relativeUri.StartsWith("/"))
                relativeUri = relativeUri.Substring(1);

            return new Uri(baseUri, relativeUri);
        }

        private Uri BuildUri(string relativeUri, string database, bool supportsMultipleTenancy, string end) //todo bad name
        {
            var baseUri = RootUri;
            if (!RootUri.AbsoluteUri.EndsWith("/"))
                baseUri = new Uri(RootUri.AbsoluteUri + "/");

            if (supportsMultipleTenancy && relativeUri.Contains("{databaseName}")) //TODO Const
            {
                if (string.IsNullOrWhiteSpace(database))
                    database = DefaultDatabase; //TODO Const
                
                relativeUri = relativeUri.Replace("{databaseName}", database);
            }

            if (relativeUri.StartsWith("/"))
                relativeUri = relativeUri.Substring(1);
            if (!string.IsNullOrWhiteSpace(end))
            {
                if (end.StartsWith("/"))
                    end = end.Substring(1);
                relativeUri += $"/{end}";
            }

            return new Uri(baseUri, relativeUri);
        }


        private string SerializeAsJson(object contents)
        {
            return Serializer.Serialize(contents);
        }

        public virtual bool IsConnected => RootApiResponse != null;

      
        CustomJsonSerializer BuildSerializer()
        {
            return new CustomJsonSerializer { JsonConverters = JsonConverters, JsonContractResolver = JsonContractResolver };
        }

        public ISerializer Serializer => new CustomJsonSerializer { JsonConverters = JsonConverters, JsonContractResolver = JsonContractResolver };

        private static string GetLastPathSegment(string uri)
        {
            var path = new Uri(uri).AbsolutePath;
            return path
                .Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .LastOrDefault();
        }

        public ICypherFluentQuery Cypher => new CypherFluentQuery(this);

        /// <inheritdoc cref="IGraphClient.DefaultDatabase"/>
        public string DefaultDatabase
        {
            get => defaultDatabase;
            set => defaultDatabase = value?.ToLowerInvariant() ?? "neo4j";
        }

        public Version ServerVersion
        {
            get
            {
                CheckRoot();
                return RootApiResponse.Version;
            }
        }

        public Uri RootEndpoint
        {
            get
            {
                CheckRoot();
                return BuildUri("");
            }
        }

        public Uri TransactionEndpoint
        {
            get
            {
                CheckRoot();
                return BuildUri(RootApiResponse.Transaction);
            }
        }

        public List<JsonConverter> JsonConverters { get; }

        private void CheckTransactionEnvironmentWithPolicy(IExecutionPolicy policy)
        {
            bool inTransaction = InTransaction;

            if (inTransaction && policy.TransactionExecutionPolicy == TransactionExecutionPolicy.Denied)
            {
                throw new InvalidOperationException("Cannot be done inside a transaction scope.");
            }

            if (!inTransaction && policy.TransactionExecutionPolicy == TransactionExecutionPolicy.Required)
            {
                throw new InvalidOperationException("Cannot be done outside a transaction scope.");
            }
        }

        public ITransaction BeginTransaction()
        {
            return BeginTransaction((IEnumerable<string>) null);
        }

        public ITransaction BeginTransaction(string bookmark)
        {
            return BeginTransaction(new List<string> {bookmark});
        }

        public ITransaction BeginTransaction(IEnumerable<string> bookmarks)
        {
            return BeginTransaction(TransactionScopeOption.Join, bookmarks, DefaultDatabase);
        }

        public ITransaction BeginTransaction(TransactionScopeOption scopeOption)
        {
            return BeginTransaction(scopeOption, null, DefaultDatabase);
        }

        public ITransaction BeginTransaction(TransactionScopeOption scopeOption, string bookmark)
        {
            return BeginTransaction(scopeOption, new List<string>{bookmark}, DefaultDatabase);
        }

        public ITransaction BeginTransaction(TransactionScopeOption scopeOption, IEnumerable<string> bookmark)
        {
            return BeginTransaction(scopeOption, bookmark, DefaultDatabase);
        }

        public ITransaction BeginTransaction(TransactionScopeOption scopeOption, IEnumerable<string> bookmarks, string database)
        {
            CheckRoot();
            if (transactionManager == null)
            {
                throw new NotSupportedException("HTTP Transactions are only supported on Neo4j 2.0 and newer.");
            }

            return transactionManager.BeginTransaction(scopeOption, bookmarks, database);
        }

        public ITransaction Transaction => transactionManager?.CurrentTransaction;

        public bool InTransaction => transactionManager != null && transactionManager.InTransaction;

        public void EndTransaction()
        {
            if (transactionManager == null)
            {
                throw new NotSupportedException("HTTP Transactions are only supported on Neo4j 2.0 and newer.");
            }
            transactionManager.EndTransaction();
        }

        public CypherCapabilities CypherCapabilities { get; private set; } = CypherCapabilities.Default;

        private async Task<CypherPartialResult> PrepareCypherRequest<TResult>(CypherQuery query, IExecutionPolicy policy)
        {
            if (InTransaction)
            {
                var response = await transactionManager
                    .EnqueueCypherRequest($"The query was: {query.QueryText}", this, query)
                    .ConfigureAwait(false);
                
                var deserializer = new CypherJsonDeserializer<TResult>(this, query.ResultMode, query.ResultFormat, true);
                return new CypherPartialResult
                {
                    DeserializationContext = deserializer.CheckForErrorsInTransactionResponse(await response.Content.ReadAsStringAsync().ConfigureAwait(false)),
                    ResponseObject = response
                };
            }

            int? maxExecutionTime = null;
            NameValueCollection customHeaders = null;
            if (query != null)
            {
                maxExecutionTime = query.MaxExecutionTime;
                customHeaders = query.CustomHeaders;
            }

            return await Request.With(ExecutionConfiguration, customHeaders, maxExecutionTime)
                .Post(policy.BaseEndpoint(query?.Database, true))
                .WithJsonContent(policy.SerializeRequest(query))
                .WithExpectedStatusCodes(HttpStatusCode.OK, HttpStatusCode.Created)
                .ExecuteAsync(response => new CypherPartialResult
                {
                    ResponseObject = response
                }).ConfigureAwait(false);
        }

        async Task<IEnumerable<TResult>> IRawGraphClient.ExecuteGetCypherResultsAsync<TResult>(CypherQuery query, CancellationToken cancellationToken)
        {
            var context = ExecutionContext.Begin(this);
            List<TResult> results;
            QueryStats stats = null;
            try
            {
                bool inTransaction = InTransaction;

                var response = await PrepareCypherRequest<TResult>(query, context.Policy).ConfigureAwait(false);
                var deserializer = new CypherJsonDeserializer<TResult>(this, query.ResultMode, query.ResultFormat, inTransaction);
                if (inTransaction)
                {
                    response.DeserializationContext.DeserializationContext.JsonContractResolver = query.JsonContractResolver;
                    results = deserializer.DeserializeFromTransactionPartialContext(response.DeserializationContext, true).ToList();
      
                }
                else
                {
                    
                    results = deserializer.Deserialize(await response.ResponseObject.Content.ReadAsStringAsync().ConfigureAwait(false), true).ToList();
                    
                }
                if (query.IncludeQueryStats)
                {
                    var responseString = await response.ResponseObject.Content.ReadAsStringAsync().ConfigureAwait(false);
                    var statsContainer = JsonConvert.DeserializeObject<QueryStatsContainer>(await response.ResponseObject.Content.ReadAsStringAsync().ConfigureAwait(false));
                    if (statsContainer != null)
                        stats = statsContainer?.Results?.FirstOrDefault()?.Stats;
                }
            }
            catch (AggregateException aggregateException)
            {
                context.Complete(query, aggregateException.TryUnwrap(out var unwrappedException) ? unwrappedException : aggregateException);
                throw;
            }
            catch (Exception e)
            {
                context.Complete(query, e);
                throw;
            }
            
            context.Complete(query, results.Count, stats);
            return results;
        }

        async Task IRawGraphClient.ExecuteCypherAsync(CypherQuery query, CancellationToken cancellationToken)
        {
            var context = ExecutionContext.Begin(this);

            CypherPartialResult response;
            try
            {
                response = await PrepareCypherRequest<object>(query, context.Policy).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                if (InTransaction)
                    ExecutionConfiguration.HasErrors = true;
                
                context.Complete(query, e);
                throw;
            }
            context.Policy.AfterExecution(TransactionHttpUtils.GetMetadataFromResponse(response.ResponseObject), null);
            QueryStats stats = null;
            if (response.ResponseObject?.Content != null)
            {
                var responseString = await response.ResponseObject.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                var errors = System.Text.Json.JsonSerializer
                    .DeserializeObject<ErrorsContainer>(responseString);
                if(errors.Errors.Any())
                    throw new ClientException(errors.Errors.First().Code, errors.Errors.First().Message);

                if (query.IncludeQueryStats)
                {
                    var statsContainer = JsonConvert.DeserializeObject<QueryStatsContainer>(responseString);
                    if (statsContainer != null)
                        stats = statsContainer?.Results?.FirstOrDefault()?.Stats;
                }
            }

            context.Complete(query, stats);
        }
        private class QueryStatsContainer
        {
            [JsonProperty("results")]
            public IList<ResultsContainer> Results { get; set; }
        }

        private class ResultsContainer
        {
            [JsonProperty("stats")]
            public QueryStats Stats { get; set; }
        }

        private class ErrorsContainer
        {
            public IList<Error> Errors { get; set; }
        }

        private class Error
        {
            [JsonProperty("code")]
            public string Code { get; set; }
            [JsonProperty("message")]
            public string Message { get; set; }
        }
        private void CheckRoot()
        {
            if (RootApiResponse == null)
                throw new InvalidOperationException("The graph client is not connected to the server. Call the Connect method first.");
        }
        
        public event OperationCompletedEventHandler OperationCompleted;

        protected void OnOperationCompleted(OperationCompletedEventArgs args)
        {
            OperationCompleted?.Invoke(this, args);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) 
                return;

            transactionManager?.Dispose();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public DefaultContractResolver JsonContractResolver { get; set; }
        public Uri GetTransactionEndpoint(string database, bool autoCommit = false)
        {
            CheckRoot();
            var uri = BuildUri(RootApiResponse.Transaction, database, RootApiResponse.Version.Major >= 4, autoCommit ? "commit" : "");
            return uri;
        }

        public ITransactionManager<HttpResponseMessage> TransactionManager => transactionManager;

#region ExecutionContext class
        private class ExecutionContext
        {
            private GraphClient owner;

            private readonly Stopwatch stopwatch;

            public IExecutionPolicy Policy { get; set; }
            public static bool HasErrors { get; set; }

            private ExecutionContext()
            {
                stopwatch = Stopwatch.StartNew();
            }

            public static ExecutionContext Begin(GraphClient owner)
            {
                owner.CheckRoot();
                var policy = owner.policyFactory.GetPolicy(PolicyType.Cypher);

                owner.CheckTransactionEnvironmentWithPolicy(policy);

                var executionContext = new ExecutionContext
                {
                    owner = owner,
                    Policy = policy
                };

                return executionContext;
            }

            public void Complete(CypherQuery query, QueryStats stats)
            {
                Complete(owner.OperationCompleted != null ? query.DebugQueryText : string.Empty, 0, null, queryStats:stats);
            }

            public void Complete(CypherQuery query)
            {
                // only parse the events when there's an event handler
                Complete(owner.OperationCompleted != null ? query.DebugQueryText : string.Empty, 0, null);
            }

            public void Complete(CypherQuery query, int resultsCount, QueryStats stats = null)
            {
                // only parse the events when there's an event handler
                Complete(owner.OperationCompleted != null ? query.DebugQueryText : string.Empty, resultsCount, null, query.CustomHeaders, queryStats:stats);
            }

            public void Complete(CypherQuery query, Exception exception)
            {
                // only parse the events when there's an event handler
                Complete(owner.OperationCompleted != null ? query.DebugQueryText : string.Empty, -1, exception);
            }

            private void Complete(string queryText, int resultsCount = -1, Exception exception = null, NameValueCollection customHeaders = null, int? maxExecutionTime = null, QueryStats queryStats = null)
            {
                var args = new OperationCompletedEventArgs
                {
                    QueryText = queryText,
                    ResourcesReturned = resultsCount,
                    TimeTaken = stopwatch.Elapsed,
                    Exception = exception,
                    CustomHeaders = customHeaders,
                    MaxExecutionTime = maxExecutionTime,
                    QueryStats = queryStats
                };

                owner.OnOperationCompleted(args);
            }
        }

#endregion
    }
}
