using System.Collections.Generic;
using System.Collections.Specialized;
using System.Threading;
using System.Threading.Tasks;
using Neo4jClient.Cypher;

namespace Neo4jClient
{
    /// <summary>
    /// These are signatures that our <see cref="GraphClient"/> exposes to
    /// support things like our Cypher infrastructure, but we don't want
    /// people to call directly. (Because there are nicer ways to do it.)
    /// </summary>
    public interface IRawGraphClient : IGraphClient
    {
  
  
        bool InTransaction { get; }
        Task<IEnumerable<TResult>> ExecuteGetCypherResultsAsync<TResult>(CypherQuery query, CancellationToken cancellationToken = default);
        Task ExecuteCypherAsync(CypherQuery query, CancellationToken cancellationToken = default);
    }
}
