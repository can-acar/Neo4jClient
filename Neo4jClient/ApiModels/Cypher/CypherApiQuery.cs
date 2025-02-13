using System.Collections.Generic;
using System.Text.Json.Serialization;
using Neo4jClient.Cypher;

namespace Neo4jClient.ApiModels.Cypher
{
    class CypherApiQuery
    {
        public CypherApiQuery(CypherQuery query)
        {
            Query = query.QueryText;
            Parameters = query.QueryParameters ?? new Dictionary<string, object>();
        }

        [JsonPropertyName("query")]
        public string Query { get; }

        [JsonPropertyName("params")]
        public IDictionary<string, object> Parameters { get; }
    }
}
