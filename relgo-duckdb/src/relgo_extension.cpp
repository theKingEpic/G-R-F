#include "relgo_extension.hpp"

#include "../../../relgo/create_index/graph_index_builder.hpp"
#include "../../../relgo/interface/op_transform.hpp"
#include "../../../relgo/relgo_statement/relgo_client_context.hpp"
#include "cypher_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#define DUCKDB_EXTENSION_MAIN

namespace duckdb {

// Create function

struct CreateGraphIndexData : public TableFunctionData {
  std::string query;
};

static unique_ptr<FunctionData>
CreateGraphIndexBind(ClientContext &, TableFunctionBindInput &input,
                     vector<LogicalType> &return_types, vector<string> &names) {
  auto result = make_uniq<CreateGraphIndexData>();
  return_types.emplace_back(LogicalType::BOOLEAN);
  names.emplace_back("Success");

  result->query = input.inputs[0].ToString();

  return std::move(result);
}

static void CreateGraphIndexFunction(duckdb::ClientContext &context,
                                     TableFunctionInput &data_p,
                                     DataChunk &output) {
  auto &relgo_context = static_cast<RelGoClientContext &>(context);
  std::fstream out_file("log.log", std::ios::app);
  auto &bind_data = data_p.bind_data->Cast<duckdb::CreateGraphIndexData>();
  std::string query = bind_data.query;
  relgo_context.SetOptimizeMode(OptimizeMode::CREATE_GRAPH_INDEX);

  relgo_context.call_query = query;
}

struct InitGraphStatsData : public TableFunctionData {};

static unique_ptr<FunctionData>
InitGraphStatsBind(ClientContext &, TableFunctionBindInput &input,
                   vector<LogicalType> &return_types, vector<string> &names) {
  auto result = make_uniq<InitGraphStatsData>();
  return_types.emplace_back(LogicalType::BOOLEAN);
  names.emplace_back("Success");

  return std::move(result);
}

static void InitGraphStatsFunction(duckdb::ClientContext &context,
                                   TableFunctionInput &data_p,
                                   DataChunk &output) {
  auto &relgo_context = static_cast<RelGoClientContext &>(context);
  relgo_context.SetOptimizeMode(OptimizeMode::INIT_GRAPH_STATS);
}

struct ExecuteGraphQueryData : public TableFunctionData {
  std::string query;
};

static unique_ptr<FunctionData>
ExecuteGraphQueryBind(ClientContext &, TableFunctionBindInput &input,
                      vector<LogicalType> &return_types,
                      vector<string> &names) {
  auto result = make_uniq<ExecuteGraphQueryData>();
  return_types.emplace_back(LogicalType::BOOLEAN);
  names.emplace_back("Success");

  result->query = input.inputs[0].ToString();

  return std::move(result);
}

static void ExecuteGraphQueryFunction(duckdb::ClientContext &context,
                                      TableFunctionInput &data_p,
                                      DataChunk &output) {
  auto &relgo_context = static_cast<RelGoClientContext &>(context);
  std::fstream out_file("log.log", std::ios::app);
  auto &bind_data = data_p.bind_data->Cast<duckdb::ExecuteGraphQueryData>();
  std::string query = bind_data.query;
  relgo_context.SetOptimizeMode(OptimizeMode::OPTIMIZE_WITH_GOPT);

  relgo_context.call_query = query;
}

struct ExplainGraphQueryData : public TableFunctionData {
  std::string query;
};

static unique_ptr<FunctionData>
ExplainGraphQueryBind(ClientContext &, TableFunctionBindInput &input,
                      vector<LogicalType> &return_types,
                      vector<string> &names) {
  auto result = make_uniq<ExplainGraphQueryData>();
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("Plan");

  result->query = input.inputs[0].ToString();

  return std::move(result);
}

static void ExplainGraphQueryFunction(duckdb::ClientContext &context,
                                      TableFunctionInput &data_p,
                                      DataChunk &output) {
  auto &relgo_context = static_cast<RelGoClientContext &>(context);
  std::fstream out_file("log.log", std::ios::app);
  auto &bind_data = data_p.bind_data->Cast<duckdb::ExplainGraphQueryData>();
  std::string query = bind_data.query;
  relgo_context.SetOptimizeMode(OptimizeMode::EXPLAIN_GRAPH_QUERY);

  relgo_context.call_query = query;
}

// LoadInternal adds the relgo functions to the database
static void LoadInternal(duckdb::DatabaseInstance &instance) {
  duckdb::Connection con(instance);
  con.BeginTransaction();
  auto &catalog = Catalog::GetSystemCatalog(*con.context);

  {
    duckdb::TableFunction create_func(
        "relgo_create_graph_index", {duckdb::LogicalType::VARCHAR},
        CreateGraphIndexFunction, CreateGraphIndexBind);
    CreateTableFunctionInfo create_info(create_func);
    catalog.CreateTableFunction(*con.context, &create_info);
  }
  {
    duckdb::TableFunction init_graph(
        "init_graph_stats", {}, InitGraphStatsFunction, InitGraphStatsBind);
    CreateTableFunctionInfo init_graph_info(init_graph);
    catalog.CreateTableFunction(*con.context, &init_graph_info);
  }
  {
    duckdb::TableFunction execute_func(
        "execute_graph_query", {duckdb::LogicalType::VARCHAR},
        ExecuteGraphQueryFunction, ExecuteGraphQueryBind);
    CreateTableFunctionInfo execute_info(execute_func);
    catalog.CreateTableFunction(*con.context, &execute_info);
  }
  {
    duckdb::TableFunction explain_func(
        "explain_graph_query", {duckdb::LogicalType::VARCHAR},
        ExplainGraphQueryFunction, ExplainGraphQueryBind);
    CreateTableFunctionInfo execute_info(explain_func);
    catalog.CreateTableFunction(*con.context, &execute_info);
  }
  {
    duckdb::TableFunction execute_cypher_func(
        "execute_cypher_query", {duckdb::LogicalType::VARCHAR},
        ExecuteCypherQueryFunction, ExecuteCypherQueryBind);
    CreateTableFunctionInfo execute_cypher(execute_cypher_func);
    catalog.CreateTableFunction(*con.context, &execute_cypher);
  }

  con.Commit();
}

void RelgoExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }

std::string RelgoExtension::Name() { return "relgo"; }

// std::string RelgoExtension::Version() const { return "0.10.0"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void relgo_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::RelgoExtension>();
}

DUCKDB_EXTENSION_API const char *relgo_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif