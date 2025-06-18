//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "../pgq2cypher/convert.hpp"
#include "../pgq2cypher/cypher_analyzer.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/enums/pending_execution_result.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/preserved_error.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/transaction/transaction_context.hpp"

namespace duckdb {
class Appender;
class Catalog;
class CatalogSearchPath;
class ColumnDataCollection;
class DatabaseInstance;
class FileOpener;
class LogicalOperator;
class PreparedStatementData;
class RelGoPreparedStatement;
class Relation;
class BufferedFileWriter;
class QueryProfiler;
class ClientContextLock;
struct CreateScalarFunctionInfo;
class ScalarFunctionCatalogEntry;
struct ActiveQueryContext;
struct ParserOptions;
struct ClientData;
class OpTransform;

enum class OptimizeMode {
  ORIGINAL,
  OPTIMIZE_WITH_GOPT,
  INIT_GRAPH_STATS,
  CREATE_GRAPH_INDEX,
  EXPLAIN_GRAPH_QUERY,
  EXECUTE_CYPHER_QUERY
};

//! The ClientContext holds information relevant to the current client session
//! during execution
class RelGoClientContext : public ClientContext {
  friend class PendingQueryResult;
  friend class StreamQueryResult;
  friend class DuckTransactionManager;

public:
  shared_ptr<RelGoClientContext> shared_from_this() {
    return std::static_pointer_cast<RelGoClientContext>(
        ClientContext::shared_from_this());
  }

public:
  DUCKDB_API explicit RelGoClientContext(shared_ptr<DatabaseInstance> db);
  DUCKDB_API ~RelGoClientContext();

  unique_ptr<PendingQueryResult> PendingQueryInternal(
      ClientContextLock &lock, unique_ptr<SQLStatement> statement,
      const PendingQueryParameters &parameters, bool verify = true);
  unique_ptr<PendingQueryResult>
  PendingStatementInternal(ClientContextLock &lock, const string &query,
                           unique_ptr<SQLStatement> statement,
                           const PendingQueryParameters &parameters);

  unique_ptr<PendingQueryResult> PendingStatementOrPreparedStatementInternal(
      ClientContextLock &lock, const string &query,
      unique_ptr<SQLStatement> statement,
      shared_ptr<PreparedStatementData> &prepared,
      const PendingQueryParameters &parameters);
  unique_ptr<PendingQueryResult> PendingStatementOrPreparedStatement(
      ClientContextLock &lock, const string &query,
      unique_ptr<SQLStatement> statement,
      shared_ptr<PreparedStatementData> &prepared,
      const PendingQueryParameters &parameters);

  //! Issue a query, returning a QueryResult. The QueryResult can be either a
  //! StreamQueryResult or a MaterializedQueryResult. The StreamQueryResult will
  //! only be returned in the case of a successful SELECT statement.
  DUCKDB_API unique_ptr<MaterializedQueryResult>
  ConnectionQuery(const string &query);
  DUCKDB_API unique_ptr<QueryResult> QueryUnlocked(ClientContextLock &lock,
                                                   const string &query,
                                                   bool allow_stream_result);
  DUCKDB_API unique_ptr<MaterializedQueryResult>
  QueryConnection(const string &query);
  DUCKDB_API unique_ptr<QueryResult> Query(const string &query,
                                           bool allow_stream_result);
  DUCKDB_API unique_ptr<QueryResult> Query(unique_ptr<SQLStatement> statement,
                                           bool allow_stream_result);
  DUCKDB_API unique_ptr<PendingQueryResult>
  PendingQuery(const string &query, bool allow_stream_result);
  //! Issues a query to the database and returns a Pending Query Result
  DUCKDB_API unique_ptr<PendingQueryResult>
  PendingQuery(unique_ptr<SQLStatement> statement, bool allow_stream_result);
  DUCKDB_API unique_ptr<PendingQueryResult>
  PendingQuery(const string &query, shared_ptr<PreparedStatementData> &prepared,
               const PendingQueryParameters &parameters);
  DUCKDB_API unique_ptr<PendingQueryResult>
  PendingQuery(const shared_ptr<Relation> &relation, bool allow_stream_result);
  DUCKDB_API unique_ptr<PendingQueryResult>
  PendingQueryPreparedInternal(ClientContextLock &lock, const string &query,
                               shared_ptr<PreparedStatementData> &prepared,
                               const PendingQueryParameters &parameters);
  unique_ptr<PendingQueryResult>
  PendingQueryInternal(ClientContextLock &,
                       const shared_ptr<Relation> &relation,
                       bool allow_stream_result);
  unique_ptr<RelGoPreparedStatement>
  PrepareInternal(ClientContextLock &lock, unique_ptr<SQLStatement> statement);
  DUCKDB_API unique_ptr<RelGoPreparedStatement> Prepare(const string &query);
  unique_ptr<RelGoPreparedStatement>
  Prepare(unique_ptr<SQLStatement> statement);

private:
  //! Internally prepare a SQL statement. Caller must hold the context_lock.
  shared_ptr<PreparedStatementData> CreatePreparedStatement(
      ClientContextLock &lock, const string &query,
      unique_ptr<SQLStatement> statement,
      optional_ptr<case_insensitive_map_t<Value>> values = nullptr);

public:
  duckdb::unique_ptr<OpTransform> op_transform;
  pgq2cypher::PGQ2CypherConverter pgq_converter;
  OptimizeMode optimize_mode;

public:
  DUCKDB_API duckdb::unique_ptr<PhysicalOperator>
  AddExplainPlan(unique_ptr<PhysicalOperator> &op);
  DUCKDB_API void SetOpTransform(duckdb::unique_ptr<OpTransform> op);
  DUCKDB_API void SetOptimizeMode(OptimizeMode mode);
  std::string call_query;
};

} // namespace duckdb
