#include "relgo_client_context.hpp"

#include "../create_index/graph_index_builder.hpp"
#include "../interface/op_transform.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/http_state.hpp"
#include "duckdb/common/preserved_error.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/statement/relation_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/pragma_handler.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "relgo_prepared_statement.hpp"
#include "relgo_statement.hpp"

namespace duckdb {

RelGoClientContext::RelGoClientContext(shared_ptr<DatabaseInstance> database)
    : ClientContext(database), optimize_mode(OptimizeMode::ORIGINAL),
      call_query("") {}

RelGoClientContext::~RelGoClientContext() {}

bool RelGoIsExplainAnalyze(SQLStatement *statement) {
  if (!statement) {
    return false;
  }
  if (statement->type != StatementType::EXPLAIN_STATEMENT) {
    return false;
  }
  auto &explain = statement->Cast<ExplainStatement>();
  return explain.explain_type == ExplainType::EXPLAIN_ANALYZE;
}

unique_ptr<PendingQueryResult> RelGoClientContext::PendingQueryInternal(
    ClientContextLock &lock, unique_ptr<SQLStatement> statement,
    const PendingQueryParameters &parameters, bool verify) {
  auto query = statement->query;
  shared_ptr<PreparedStatementData> prepared;
  if (verify) {
    return PendingStatementOrPreparedStatementInternal(
        lock, query, std::move(statement), prepared, parameters);
  } else {
    return PendingStatementOrPreparedStatement(
        lock, query, std::move(statement), prepared, parameters);
  }
}

unique_ptr<PendingQueryResult>
RelGoClientContext::PendingStatementOrPreparedStatementInternal(
    ClientContextLock &lock, const string &query,
    unique_ptr<SQLStatement> statement,
    shared_ptr<PreparedStatementData> &prepared,
    const PendingQueryParameters &parameters) {
  // check if we are on AutoCommit. In this case we should start a transaction.
  if (statement && config.AnyVerification()) {
    // query verification is enabled
    // create a copy of the statement, and use the copy
    // this way we verify that the copy correctly copies all properties
    auto copied_statement = statement->Copy();
    switch (statement->type) {
    case StatementType::SELECT_STATEMENT: {
      // in case this is a select query, we verify the original statement
      PreservedError error;
      try {
        error = VerifyQuery(lock, query, std::move(statement));
      } catch (const Exception &ex) {
        error = PreservedError(ex);
      } catch (std::exception &ex) {
        error = PreservedError(ex);
      }
      if (error) {
        // error in verifying query
        return make_uniq<PendingQueryResult>(error);
      }
      statement = std::move(copied_statement);
      break;
    }
#ifndef DUCKDB_ALTERNATIVE_VERIFY
    case StatementType::COPY_STATEMENT:
    case StatementType::INSERT_STATEMENT:
    case StatementType::DELETE_STATEMENT:
    case StatementType::UPDATE_STATEMENT: {
      Parser parser;
      PreservedError error;
      try {
        parser.ParseQuery(statement->ToString());
      } catch (const Exception &ex) {
        error = PreservedError(ex);
      } catch (std::exception &ex) {
        error = PreservedError(ex);
      }
      if (error) {
        // error in verifying query
        return make_uniq<PendingQueryResult>(error);
      }
      statement = std::move(parser.statements[0]);
      break;
    }
#endif
    default:
      statement = std::move(copied_statement);
      break;
    }
  }
  return PendingStatementOrPreparedStatement(lock, query, std::move(statement),
                                             prepared, parameters);
}

unique_ptr<PendingQueryResult>
RelGoClientContext::PendingStatementOrPreparedStatement(
    ClientContextLock &lock, const string &query,
    unique_ptr<SQLStatement> statement,
    shared_ptr<PreparedStatementData> &prepared,
    const PendingQueryParameters &parameters) {
  unique_ptr<PendingQueryResult> result;

  try {
    BeginQueryInternal(lock, query);
  } catch (FatalException &ex) {
    // fatal exceptions invalidate the entire database
    auto &db = DatabaseInstance::GetDatabase(*this);
    ValidChecker::Invalidate(db, ex.what());
    result = make_uniq<PendingQueryResult>(PreservedError(ex));
    return result;
  } catch (const Exception &ex) {
    return make_uniq<PendingQueryResult>(PreservedError(ex));
  } catch (std::exception &ex) {
    return make_uniq<PendingQueryResult>(PreservedError(ex));
  }
  // start the profiler
  auto &profiler = QueryProfiler::Get(*this);
  profiler.StartQuery(
      query,
      RelGoIsExplainAnalyze(statement ? statement.get()
                                      : prepared->unbound_statement.get()));

  bool invalidate_query = true;
  try {
    if (statement) {
      result = PendingStatementInternal(lock, query, std::move(statement),
                                        parameters);
    } else {
      if (prepared->RequireRebind(*this, parameters.parameters)) {
        // catalog was modified: rebind the statement before execution
        auto new_prepared = CreatePreparedStatement(
            lock, query, prepared->unbound_statement->Copy(),
            parameters.parameters);
        D_ASSERT(new_prepared->properties.bound_all_parameters);
        new_prepared->unbound_statement =
            std::move(prepared->unbound_statement);
        prepared = std::move(new_prepared);
        prepared->properties.bound_all_parameters = false;
      }
      result = PendingPreparedStatement(lock, prepared, parameters);
    }
  } catch (StandardException &ex) {
    // standard exceptions do not invalidate the current transaction
    result = make_uniq<PendingQueryResult>(PreservedError(ex));
    invalidate_query = false;
  } catch (FatalException &ex) {
    // fatal exceptions invalidate the entire database
    if (!config.query_verification_enabled) {
      auto &db = DatabaseInstance::GetDatabase(*this);
      ValidChecker::Invalidate(db, ex.what());
    }
    result = make_uniq<PendingQueryResult>(PreservedError(ex));
  } catch (const Exception &ex) {
    // other types of exceptions do invalidate the current transaction
    result = make_uniq<PendingQueryResult>(PreservedError(ex));
  } catch (std::exception &ex) {
    // other types of exceptions do invalidate the current transaction
    result = make_uniq<PendingQueryResult>(PreservedError(ex));
  }
  if (result->HasError()) {
    // query failed: abort now
    EndQueryInternal(lock, false, invalidate_query);
    return result;
  }
  // D_ASSERT(active_query->open_result == result.get());
  return result;
}

unique_ptr<PendingQueryResult> RelGoClientContext::PendingStatementInternal(
    ClientContextLock &lock, const string &query,
    unique_ptr<SQLStatement> statement,
    const PendingQueryParameters &parameters) {
  // prepare the query for execution
  auto prepared = CreatePreparedStatement(lock, query, std::move(statement),
                                          parameters.parameters);
  idx_t parameter_count =
      !parameters.parameters ? 0 : parameters.parameters->size();
  if (prepared->properties.parameter_count > 0 && parameter_count == 0) {
    string error_message =
        StringUtil::Format("Expected %lld parameters, but none were supplied",
                           prepared->properties.parameter_count);
    return make_uniq<PendingQueryResult>(PreservedError(error_message));
  }
  if (!prepared->properties.bound_all_parameters) {
    return make_uniq<PendingQueryResult>(
        PreservedError("Not all parameters were bound"));
  }
  // execute the prepared statement
  return PendingPreparedStatement(lock, std::move(prepared), parameters);
}

unique_ptr<QueryResult>
RelGoClientContext::Query(unique_ptr<SQLStatement> statement,
                          bool allow_stream_result) {
  std::fstream out_file("log.log", std::ios::app);
  out_file << statement->query << std::endl;
  out_file << "query statement" << std::endl;

  auto pending_query = PendingQuery(std::move(statement), allow_stream_result);
  if (pending_query->HasError()) {
    return make_uniq<MaterializedQueryResult>(pending_query->GetErrorObject());
  }
  return pending_query->Execute();
}

unique_ptr<MaterializedQueryResult>
RelGoClientContext::QueryConnection(const string &query) {
  auto result = Query(query, false);
  D_ASSERT(result->type == QueryResultType::MATERIALIZED_RESULT);
  return unique_ptr_cast<QueryResult, MaterializedQueryResult>(
      std::move(result));
}

unique_ptr<QueryResult> RelGoClientContext::Query(const string &query,
                                                  bool allow_stream_result) {
  auto lock = LockContext();

  PreservedError error;
  vector<unique_ptr<SQLStatement>> statements;
  if (!ParseStatements(*lock, query, statements, error)) {
    if (optimize_mode == OptimizeMode::ORIGINAL) {
      return make_uniq<MaterializedQueryResult>(std::move(error));
    } else {
      unique_ptr<RelGoStatement> stat = make_uniq<RelGoStatement>();
      stat->query = query;
      statements.push_back(move(stat));
    }
  }
  // if (!ParseStatements(*lock, query, statements, error)) {
  // 	return make_uniq<MaterializedQueryResult>(std::move(error));
  // }
  if (statements.empty()) {
    // no statements, return empty successful result
    StatementProperties properties;
    vector<string> names;
    auto collection =
        make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
    return make_uniq<MaterializedQueryResult>(
        StatementType::INVALID_STATEMENT, properties, std::move(names),
        std::move(collection), GetClientProperties());
  }

  unique_ptr<QueryResult> result;
  QueryResult *last_result = nullptr;
  bool last_had_result = false;
  for (idx_t i = 0; i < statements.size(); i++) {
    auto &statement = statements[i];
    bool is_last_statement = i + 1 == statements.size();
    PendingQueryParameters parameters;
    parameters.allow_stream_result = allow_stream_result && is_last_statement;
    auto pending_query =
        PendingQueryInternal(*lock, std::move(statement), parameters);
    auto has_result = pending_query->properties.return_type ==
                      StatementReturnType::QUERY_RESULT;
    unique_ptr<QueryResult> current_result;
    if (pending_query->HasError()) {
      current_result =
          make_uniq<MaterializedQueryResult>(pending_query->GetErrorObject());
    } else {
      current_result = ExecutePendingQueryInternal(*lock, *pending_query);
    }
    // now append the result to the list of results
    if (!last_result || !last_had_result) {
      // first result of the query
      result = std::move(current_result);
      last_result = result.get();
      last_had_result = has_result;
    } else {
      // later results; attach to the result chain
      // but only if there is a result
      if (!has_result) {
        continue;
      }
      last_result->next = std::move(current_result);
      last_result = last_result->next.get();
    }
  }

  return result;
}

shared_ptr<PreparedStatementData> RelGoClientContext::CreatePreparedStatement(
    ClientContextLock &lock, const string &query,
    unique_ptr<SQLStatement> statement,
    optional_ptr<case_insensitive_map_t<Value>> values) {
  StatementType statement_type = statement->type;
  auto result = make_shared<PreparedStatementData>(statement_type);

  if (optimize_mode == OptimizeMode::ORIGINAL) {
    auto &profiler = QueryProfiler::Get(*this);
    profiler.StartQuery(query, RelGoIsExplainAnalyze(statement.get()), true);
    profiler.StartPhase("planner");
    Planner planner(*this);
    if (values) {
      auto &parameter_values = *values;
      for (auto &value : parameter_values) {
        planner.parameter_data.emplace(value.first,
                                       BoundParameterData(value.second));
      }
    }

    client_data->http_state = make_shared<HTTPState>();
    planner.CreatePlan(std::move(statement));
    D_ASSERT(planner.plan || !planner.properties.bound_all_parameters);
    profiler.EndPhase();

    auto plan = std::move(planner.plan);
    // extract the result column names from the plan

    result->properties = planner.properties;
    result->names = planner.names;
    result->types = planner.types;

    if (relgo::startWith(query, "CALL explain")) {
      result->names = {"explain_key", "explain_value"};
      result->types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
    }
    result->value_map = std::move(planner.value_map);
    result->catalog_version = MetaTransaction::Get(*this).catalog_version;

    if (!planner.properties.bound_all_parameters) {
      return result;
    }
#ifdef DEBUG
    plan->Verify(*this);
#endif
    if (config.enable_optimizer && plan->RequireOptimizer()) {
      profiler.StartPhase("optimizer");

      // start_time =
      // std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

      Optimizer optimizer(*planner.binder, *this);
      plan = optimizer.Optimize(std::move(plan));

      // end_time =
      // std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

      D_ASSERT(plan);
      profiler.EndPhase();

#ifdef DEBUG
      plan->Verify(*this);
#endif
    }

    profiler.StartPhase("physical_planner");
    // now convert logical query plan into a physical query plan
    PhysicalPlanGenerator physical_planner(*this);
    auto physical_plan = physical_planner.CreatePlan(std::move(plan));
    // std::cout << physical_plan->ToString() << std::endl;
    profiler.EndPhase();

    result->plan = std::move(physical_plan);
    return result;
  } else {
    if (optimize_mode == OptimizeMode::CREATE_GRAPH_INDEX) {
      relgo::GraphIndexBuilder builder(*this);
      relgo::GraphIndexBuildInfo build_info(query);
      auto physical_plan = builder.generateGraphIndexPlan(build_info);

      result->plan = std::move(physical_plan);
      result->names = duckdb::vector<std::string>(1, "Success");
      result->types =
          duckdb::vector<duckdb::LogicalType>(1, duckdb::LogicalType::BIGINT);
      result->catalog_version = MetaTransaction::Get(*this).catalog_version;

      result->properties.requires_valid_transaction = true;
      result->properties.allow_stream_result = true;
      result->properties.bound_all_parameters = true;
      result->properties.return_type = duckdb::StatementReturnType::NOTHING;
      result->properties.parameter_count = 0;
      return result;
    } else if (optimize_mode == OptimizeMode::EXECUTE_CYPHER_QUERY) {
      PhysicalPlanGenerator physical_planner(*this);

      std::string cypher_query = query;
      std::vector<std::string> return_cols =
          CypherAnalyzer::extractReturn(cypher_query);

      std::cout << "get column ";
      for (const auto &col : return_cols) {
        std::cout << col << " ";
      }
      std::cout << std::endl;

      std::chrono::time_point<std::chrono::system_clock,
                              std::chrono::milliseconds>
          end_1 = std::chrono::time_point_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now());
      // if (query[0] == 'S' || query[0] == 's') {
      // execution_query.push_back(query);

      auto physical_plan = move(op_transform->optimize(cypher_query));

      result->plan = std::move(physical_plan);
      result->names =
          duckdb::vector<std::string>(return_cols.begin(), return_cols.end());
      result->types = result->plan->types;
      result->catalog_version = MetaTransaction::Get(*this).catalog_version;

      result->properties.requires_valid_transaction = true;
      result->properties.allow_stream_result = true;
      result->properties.bound_all_parameters = true;
      result->properties.return_type =
          duckdb::StatementReturnType::QUERY_RESULT;
      result->properties.parameter_count = 0;

      // result->properties.bound_all_parameters = true;

      return result;
    } else if (optimize_mode == OptimizeMode::OPTIMIZE_WITH_GOPT ||
               optimize_mode == OptimizeMode::EXPLAIN_GRAPH_QUERY) {
      PhysicalPlanGenerator physical_planner(*this);

      std::chrono::time_point<std::chrono::system_clock,
                              std::chrono::milliseconds>
          start_1 = std::chrono::time_point_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now());
      // if (query[0] == 'S' || query[0] == 's') {
      // execution_query.push_back(query);

      std::cout << "convert " << query << std::endl;
      auto [cypher_query, return_cols] = pgq_converter.convert(query);
      std::cout << "get query " << cypher_query << std::endl;

      std::chrono::time_point<std::chrono::system_clock,
                              std::chrono::milliseconds>
          end_1 = std::chrono::time_point_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now());
      // if (query[0] == 'S' || query[0] == 's') {
      // execution_query.push_back(query);
      std::cout << "SQL to Cypher TIME: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       end_1 - start_1)
                       .count()
                << std::endl;

      auto physical_plan = move(op_transform->optimize(cypher_query));

      if (optimize_mode == OptimizeMode::OPTIMIZE_WITH_GOPT) {
        result->plan = std::move(physical_plan);
        result->names =
            duckdb::vector<std::string>(return_cols.begin(), return_cols.end());
        result->types = result->plan->types;
        result->catalog_version = MetaTransaction::Get(*this).catalog_version;

        result->properties.requires_valid_transaction = true;
        result->properties.allow_stream_result = true;
        result->properties.bound_all_parameters = true;
        result->properties.return_type =
            duckdb::StatementReturnType::QUERY_RESULT;
        result->properties.parameter_count = 0;

        // result->properties.bound_all_parameters = true;

        return result;
      } else {
        auto result = make_shared<PreparedStatementData>(
            StatementType::EXPLAIN_STATEMENT);

        result->plan = AddExplainPlan(physical_plan);
        result->names = {"explain_key", "explain_value"};
        result->types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
        result->catalog_version = MetaTransaction::Get(*this).catalog_version;

        result->properties.requires_valid_transaction = true;
        result->properties.allow_stream_result = true;
        result->properties.bound_all_parameters = true;

        result->properties.return_type =
            duckdb::StatementReturnType::QUERY_RESULT;
        // result->properties.bound_all_parameters = true;

        result->properties.parameter_count = 0;

        return result;
      }
    }
  }

  result->plan = nullptr;
  return result;
}

unique_ptr<PendingQueryResult>
RelGoClientContext::PendingQuery(const string &query,
                                 shared_ptr<PreparedStatementData> &prepared,
                                 const PendingQueryParameters &parameters) {
  std::fstream out_file("log.log", std::ios::app);
  out_file << "reach relgo client context: " << query << std::endl;
  auto lock = LockContext();
  return PendingQueryPreparedInternal(*lock, query, prepared, parameters);
}

unique_ptr<PendingQueryResult>
RelGoClientContext::PendingQuery(const string &query,
                                 bool allow_stream_result) {
  auto lock = LockContext();

  PreservedError error;
  vector<unique_ptr<SQLStatement>> statements;
  if (!ParseStatements(*lock, query, statements, error)) {
    return make_uniq<PendingQueryResult>(std::move(error));
  }
  if (statements.size() != 1) {
    return make_uniq<PendingQueryResult>(
        PreservedError("PendingQuery can only take a single statement"));
  }
  PendingQueryParameters parameters;
  parameters.allow_stream_result = allow_stream_result;
  return PendingQueryInternal(*lock, std::move(statements[0]), parameters);
}

unique_ptr<PendingQueryResult>
RelGoClientContext::PendingQuery(unique_ptr<SQLStatement> statement,
                                 bool allow_stream_result) {
  auto lock = LockContext();
  PendingQueryParameters parameters;
  parameters.allow_stream_result = allow_stream_result;
  return PendingQueryInternal(*lock, std::move(statement), parameters);
}

unique_ptr<PendingQueryResult>
RelGoClientContext::PendingQuery(const shared_ptr<Relation> &relation,
                                 bool allow_stream_result) {
  auto lock = LockContext();
  return PendingQueryInternal(*lock, relation, allow_stream_result);
}

unique_ptr<PendingQueryResult> RelGoClientContext::PendingQueryPreparedInternal(
    ClientContextLock &lock, const string &query,
    shared_ptr<PreparedStatementData> &prepared,
    const PendingQueryParameters &parameters) {
  try {
    InitialCleanup(lock);
  } catch (const Exception &ex) {
    return make_uniq<PendingQueryResult>(PreservedError(ex));
  } catch (std::exception &ex) {
    return make_uniq<PendingQueryResult>(PreservedError(ex));
  }
  return PendingStatementOrPreparedStatementInternal(lock, query, nullptr,
                                                     prepared, parameters);
}

unique_ptr<PendingQueryResult>
RelGoClientContext::PendingQueryInternal(ClientContextLock &lock,
                                         const shared_ptr<Relation> &relation,
                                         bool allow_stream_result) {
  InitialCleanup(lock);

  string query;
  if (config.query_verification_enabled) {
    // run the ToString method of any relation we run, mostly to ensure it
    // doesn't crash
    relation->ToString();
    relation->GetAlias();
    if (relation->IsReadOnly()) {
      // verify read only statements by running a select statement
      auto select = make_uniq<SelectStatement>();
      select->node = relation->GetQueryNode();
      RunStatementInternal(lock, query, std::move(select), false);
    }
  }

  auto relation_stmt = make_uniq<RelationStatement>(relation);
  PendingQueryParameters parameters;
  parameters.allow_stream_result = allow_stream_result;
  return PendingQueryInternal(lock, std::move(relation_stmt), parameters);
}

unique_ptr<RelGoPreparedStatement>
RelGoClientContext::PrepareInternal(ClientContextLock &lock,
                                    unique_ptr<SQLStatement> statement) {
  auto n_param = statement->n_param;
  auto named_param_map = std::move(statement->named_param_map);
  auto statement_query = statement->query;
  shared_ptr<PreparedStatementData> prepared_data;
  auto unbound_statement = statement->Copy();
  RunFunctionInTransactionInternal(
      lock,
      [&]() {
        prepared_data = CreatePreparedStatement(lock, statement_query,
                                                std::move(statement));
      },
      false);
  prepared_data->unbound_statement = std::move(unbound_statement);
  return make_uniq<RelGoPreparedStatement>(
      shared_from_this(), std::move(prepared_data), std::move(statement_query),
      n_param, std::move(named_param_map));
}

unique_ptr<RelGoPreparedStatement>
RelGoClientContext::Prepare(unique_ptr<SQLStatement> statement) {
  auto lock = LockContext();
  // prepare the query
  try {
    InitialCleanup(*lock);
    return PrepareInternal(*lock, std::move(statement));
  } catch (const Exception &ex) {
    return make_uniq<RelGoPreparedStatement>(PreservedError(ex));
  } catch (std::exception &ex) {
    return make_uniq<RelGoPreparedStatement>(PreservedError(ex));
  }
}

unique_ptr<RelGoPreparedStatement>
RelGoClientContext::Prepare(const string &query) {
  auto lock = LockContext();
  // prepare the query
  try {
    InitialCleanup(*lock);

    // first parse the query
    auto statements = ParseStatementsInternal(*lock, query);
    if (statements.empty()) {
      throw Exception("No statement to prepare!");
    }
    if (statements.size() > 1) {
      throw Exception("Cannot prepare multiple statements at once!");
    }
    return PrepareInternal(*lock, std::move(statements[0]));
  } catch (const Exception &ex) {
    return make_uniq<RelGoPreparedStatement>(PreservedError(ex));
  } catch (std::exception &ex) {
    return make_uniq<RelGoPreparedStatement>(PreservedError(ex));
  }
}

duckdb::unique_ptr<PhysicalOperator>
RelGoClientContext::AddExplainPlan(unique_ptr<PhysicalOperator> &plan) {
  std::string logical_plan_opt = "";
  std::string physical_plan = plan->ToString();
  std::string logical_plan_unopt = "";
  vector<LogicalType> types = {LogicalType::VARCHAR, LogicalType::VARCHAR};

  // the output of the explain
  vector<string> keys, values;
  switch (ClientConfig::GetConfig(*this).explain_output_type) {
  case ExplainOutputType::OPTIMIZED_ONLY:
    keys = {"logical_opt"};
    values = {logical_plan_opt};
    break;
  case ExplainOutputType::PHYSICAL_ONLY:
    keys = {"physical_plan"};
    values = {physical_plan};
    break;
  default:
    keys = {"logical_plan", "logical_opt", "physical_plan"};
    values = {logical_plan_unopt, logical_plan_opt, physical_plan};
  }

  // create a ColumnDataCollection from the output
  auto &allocator = Allocator::Get(*this);
  vector<LogicalType> plan_types{LogicalType::VARCHAR, LogicalType::VARCHAR};
  auto collection = make_uniq<ColumnDataCollection>(
      *this, plan_types, ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR);

  DataChunk chunk;
  chunk.Initialize(allocator, types);
  for (idx_t i = 0; i < keys.size(); i++) {
    chunk.SetValue(0, chunk.size(), Value(keys[i]));
    chunk.SetValue(1, chunk.size(), Value(values[i]));
    chunk.SetCardinality(chunk.size() + 1);
    if (chunk.size() == STANDARD_VECTOR_SIZE) {
      collection->Append(chunk);
      chunk.Reset();
    }
  }
  collection->Append(chunk);

  // create a chunk scan to output the result
  auto chunk_scan = make_uniq<PhysicalColumnDataScan>(
      types, PhysicalOperatorType::COLUMN_DATA_SCAN, 1, std::move(collection));
  return std::move(chunk_scan);
}

void RelGoClientContext::SetOpTransform(duckdb::unique_ptr<OpTransform> op) {
  op_transform = move(op);
}

void RelGoClientContext::SetOptimizeMode(OptimizeMode mode) {
  optimize_mode = mode;
}
} // namespace duckdb