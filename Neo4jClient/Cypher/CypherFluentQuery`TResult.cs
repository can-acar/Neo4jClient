using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Neo4jClient.Cypher
{
    [DebuggerDisplay("{Query.DebugQueryText}")]
    public class CypherFluentQuery<TResult> :
        CypherFluentQuery,
        IOrderedCypherFluentQuery<TResult>
    {
        public CypherFluentQuery(IGraphClient client, QueryWriter writer, bool isWrite = true, bool includeQueryStats = false)
            : base(client, writer, isWrite, includeQueryStats)
        {}

        public async Task<IEnumerable<TResult>> QueryResultAsync(CancellationToken cancellationToken = default)
        {
            var results = await ExecuteAsync(cancellationToken).ConfigureAwait(false);
            return results;
        }
       

        public new ICypherFluentQuery<TResult> Unwind(string collectionName, string columnName)
        {
            return Mutate<TResult>(w => w.AppendClause($"UNWIND {collectionName} AS {columnName}"));
        }

        public new ICypherFluentQuery<TResult> Limit(int? limit)
        {
            return limit.HasValue
                ? Mutate<TResult>(w => w.AppendClause("LIMIT {0}", limit))
                : this;
        }

        public new ICypherFluentQuery<TResult> Skip(int? skip)
        {
            return skip.HasValue
                ? Mutate<TResult>(w => w.AppendClause("SKIP {0}", skip))
                : this;
        }

        public new IOrderedCypherFluentQuery<TResult> OrderBy(params string[] properties)
        {
            return MutateOrdered<TResult>(w =>
                w.AppendClause($"ORDER BY {string.Join(", ", properties)}"));
        }

        public new IOrderedCypherFluentQuery<TResult> OrderByDescending(params string[] properties)
        {
            return MutateOrdered<TResult>(w =>
                w.AppendClause($"ORDER BY {string.Join(" DESC, ", properties)} DESC"));
        }

        public new IOrderedCypherFluentQuery<TResult> ThenBy(params string[] properties)
        {
            return MutateOrdered<TResult>(w =>
                w.AppendToClause($", {string.Join(", ", properties)}"));
        }

        public new IOrderedCypherFluentQuery<TResult> ThenByDescending(params string[] properties)
        {
            return MutateOrdered<TResult>(w =>
                w.AppendToClause($", {string.Join(" DESC, ", properties)} DESC"));
        }

        public Task<IEnumerable<TResult>> ResultsAsync => Client.ExecuteGetCypherResultsAsync<TResult>(Query);

        private CypherQuery BuildQuery()
        {
            // Get the query text and parameters from the QueryWriter
            var queryText = Client.Cypher.Query.QueryText;
            var queryParams = Client.Cypher.Query.QueryParameters;

            // Create a new CypherQuery with the accumulated information
            return new CypherQuery(
                queryText,
                queryParams,
                QueryWriter.ResultMode,
                QueryWriter.ResultFormat,
                QueryWriter.DatabaseName);

        }
        protected virtual async Task<IEnumerable<TResult>> ExecuteAsync(CancellationToken cancellationToken)
        {
            var query = BuildQuery();
            var results = await ((IRawGraphClient)Client).ExecuteGetCypherResultsAsync<TResult>(query, cancellationToken);
            return results;
        }
       
    }
}
