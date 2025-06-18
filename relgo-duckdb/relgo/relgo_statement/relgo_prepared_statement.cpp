#include "relgo_prepared_statement.hpp"

#include "../interface/op_transform.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "relgo_client_context.hpp"

#include <fstream>
#include <iostream>

namespace duckdb {

RelGoPreparedStatement::RelGoPreparedStatement(
    shared_ptr<RelGoClientContext> context,
    shared_ptr<PreparedStatementData> data_p, string query, idx_t n_param,
    case_insensitive_map_t<idx_t> named_param_map_p)
    : context(std::move(context)),
      PreparedStatement(nullptr, data_p, query, n_param, named_param_map_p) {
  D_ASSERT(data || !success);
}

RelGoPreparedStatement::RelGoPreparedStatement(PreservedError error)
    : PreparedStatement(error) {}

RelGoPreparedStatement::~RelGoPreparedStatement() {}

const string &RelGoPreparedStatement::GetError() {
  D_ASSERT(HasError());
  return error.Message();
}

PreservedError &RelGoPreparedStatement::GetErrorObject() { return error; }

bool RelGoPreparedStatement::HasError() const { return !success; }

idx_t RelGoPreparedStatement::ColumnCount() {
  D_ASSERT(data);
  return data->types.size();
}

StatementType RelGoPreparedStatement::GetStatementType() {
  D_ASSERT(data);
  return data->statement_type;
}

StatementProperties RelGoPreparedStatement::GetStatementProperties() {
  D_ASSERT(data);
  return data->properties;
}

const vector<LogicalType> &RelGoPreparedStatement::GetTypes() {
  D_ASSERT(data);
  return data->types;
}

const vector<string> &RelGoPreparedStatement::GetNames() {
  D_ASSERT(data);
  return data->names;
}

case_insensitive_map_t<LogicalType>
RelGoPreparedStatement::GetExpectedParameterTypes() const {
  D_ASSERT(data);
  case_insensitive_map_t<LogicalType> expected_types(data->value_map.size());
  for (auto &it : data->value_map) {
    auto &identifier = it.first;
    D_ASSERT(data->value_map.count(identifier));
    D_ASSERT(it.second);
    expected_types[identifier] = it.second->GetValue().type();
  }
  return expected_types;
}

unique_ptr<QueryResult>
RelGoPreparedStatement::Execute(case_insensitive_map_t<Value> &named_values,
                                bool allow_stream_result) {
  auto pending = PendingQuery(named_values, allow_stream_result);
  if (pending->HasError()) {
    return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
  }
  return pending->Execute();
}

unique_ptr<QueryResult>
RelGoPreparedStatement::Execute(vector<Value> &values,
                                bool allow_stream_result) {
  auto pending = PendingQuery(values, allow_stream_result);
  if (pending->HasError()) {
    return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
  }
  auto result = pending->Execute();

  if (context->optimize_mode == OptimizeMode::ORIGINAL) {
    return result;
  } else if (context->optimize_mode == OptimizeMode::CREATE_GRAPH_INDEX) {
    // context->SetOptimizeMode(OptimizeMode::ORIGINAL);
    // context->call_query = "SELECT * FROM person LIMIT 10;";
    auto final_result = context->Query(context->call_query, false);

    auto &show_result = (MaterializedQueryResult &)*final_result;
    // out << show_result.ToString() << std::endl;
    context->SetOptimizeMode(OptimizeMode::ORIGINAL);

    return final_result;
  } else if (context->optimize_mode == OptimizeMode::INIT_GRAPH_STATS) {
    context->SetOptimizeMode(OptimizeMode::ORIGINAL);
    duckdb::unique_ptr<OpTransform> op = make_uniq<OpTransform>(context);
    op->init();
    context->SetOpTransform(move(op));
    return result;
  } else if (context->optimize_mode == OptimizeMode::OPTIMIZE_WITH_GOPT) {
    auto final_result = context->Query(context->call_query, false);

    auto &show_result = (MaterializedQueryResult &)*final_result;
    // out << show_result.ToString() << std::endl;
    context->SetOptimizeMode(OptimizeMode::ORIGINAL);

    return final_result;
  } else if (context->optimize_mode == OptimizeMode::EXPLAIN_GRAPH_QUERY) {
    auto final_result = context->Query(context->call_query, false);

    auto &show_result = (MaterializedQueryResult &)*final_result;
    // out << show_result.ToString() << std::endl;
    context->SetOptimizeMode(OptimizeMode::ORIGINAL);

    return final_result;
  } else if (context->optimize_mode == OptimizeMode::EXECUTE_CYPHER_QUERY) {
    auto final_result = context->Query(context->call_query, false);

    auto &show_result = (MaterializedQueryResult &)*final_result;
    // out << show_result.ToString() << std::endl;
    context->SetOptimizeMode(OptimizeMode::ORIGINAL);

    return final_result;
  } else {
    std::cout << "change to unknown mode" << std::endl;
  }

  return result;
}

unique_ptr<PendingQueryResult>
RelGoPreparedStatement::PendingQuery(vector<Value> &values,
                                     bool allow_stream_result) {
  case_insensitive_map_t<Value> named_values;
  for (idx_t i = 0; i < values.size(); i++) {
    auto &val = values[i];
    named_values[std::to_string(i + 1)] = val;
  }
  return PendingQuery(named_values, allow_stream_result);
}

unique_ptr<PendingQueryResult> RelGoPreparedStatement::PendingQuery(
    case_insensitive_map_t<Value> &named_values, bool allow_stream_result) {
  if (!success) {
    auto exception = InvalidInputException(
        "Attempting to execute an unsuccessfully prepared statement!");
    return make_uniq<PendingQueryResult>(PreservedError(exception));
  }
  PendingQueryParameters parameters;
  parameters.parameters = &named_values;

  try {
    VerifyParameters(named_values, named_param_map);
  } catch (const Exception &ex) {
    return make_uniq<PendingQueryResult>(PreservedError(ex));
  }

  D_ASSERT(data);
  parameters.allow_stream_result =
      allow_stream_result && data->properties.allow_stream_result;
  auto result = context->PendingQuery(query, data, parameters);
  // The result should not contain any reference to the 'vector<Value>
  // parameters.parameters'
  return result;
}

} // namespace duckdb
