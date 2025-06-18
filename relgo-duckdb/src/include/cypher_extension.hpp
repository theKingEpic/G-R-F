#pragma once
#include "../../../relgo/relgo_statement/relgo_client_context.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension.hpp"

namespace duckdb {

struct ExecuteCypherQueryData : public TableFunctionData {
  std::string query;
};

static unique_ptr<FunctionData>
ExecuteCypherQueryBind(ClientContext &, TableFunctionBindInput &input,
                       vector<LogicalType> &return_types,
                       vector<string> &names) {
  auto result = make_uniq<ExecuteCypherQueryData>();
  return_types.emplace_back(LogicalType::BOOLEAN);
  names.emplace_back("Success");

  result->query = input.inputs[0].ToString();

  return std::move(result);
}

static void ExecuteCypherQueryFunction(duckdb::ClientContext &context,
                                       TableFunctionInput &data_p,
                                       DataChunk &output) {
  auto &relgo_context = static_cast<RelGoClientContext &>(context);
  auto &bind_data = data_p.bind_data->Cast<duckdb::ExecuteCypherQueryData>();
  std::string query = bind_data.query;
  relgo_context.SetOptimizeMode(OptimizeMode::EXECUTE_CYPHER_QUERY);

  relgo_context.call_query = query;
}

} // namespace duckdb