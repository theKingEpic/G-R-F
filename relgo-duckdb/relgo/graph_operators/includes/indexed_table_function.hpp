//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/indexed_table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "../../graph_index/alist.hpp"
#include "../includes/indexed_row_group_collection.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

#include <functional>

namespace duckdb {

class BaseStatistics;
class DependencyList;
class LogicalGet;
class TableFilterSet;

struct IndexedTableFunctionInitInputRAI {
  IndexedTableFunctionInitInputRAI(
      Vector *rid_vector_p, DataChunk *rai_chunk_p,
      const shared_ptr<relgo::rows_vector> &rows_filter_p,
      const shared_ptr<relgo::bitmask_vector> &row_bitmask_p,
      const shared_ptr<relgo::bitmask_vector> &zone_bitmask_p,
      row_t rows_count_p, TableFilterSet *table_filters_p,
      shared_ptr<ExpressionExecutor> executor_p, SelectionVector *sel_p)
      : rid_vector(rid_vector_p), rai_chunk(rai_chunk_p),
        rows_filter(rows_filter_p), row_bitmask(row_bitmask_p),
        zone_bitmask(zone_bitmask_p), rows_count(rows_count_p),
        table_filters(table_filters_p), executor(executor_p), sel(sel_p),
        indexed_collection(nullptr), indexed_local_collection(nullptr) {}
  IndexedTableFunctionInitInputRAI()
      : rid_vector(nullptr), rai_chunk(nullptr), rows_filter(nullptr),
        row_bitmask(nullptr), zone_bitmask(nullptr), rows_count(-1),
        table_filters(nullptr), executor(nullptr), sel(nullptr),
        indexed_collection(nullptr), indexed_local_collection(nullptr) {}

  //! rid_vector
  Vector *rid_vector;
  DataChunk *rai_chunk;
  //! rows_count
  shared_ptr<relgo::rows_vector> rows_filter;
  shared_ptr<relgo::bitmask_vector> row_bitmask;
  shared_ptr<relgo::bitmask_vector> zone_bitmask;
  row_t rows_count;
  TableFilterSet *table_filters;
  shared_ptr<ExpressionExecutor> executor;
  SelectionVector *sel;
  shared_ptr<IndexedRowGroupCollection> indexed_collection;
  shared_ptr<IndexedRowGroupCollection> indexed_local_collection;
};

struct IndexedTableFunctionInitInput {
  IndexedTableFunctionInitInput(optional_ptr<const FunctionData> bind_data_p,
                                const vector<column_t> &column_ids_p,
                                const vector<idx_t> &projection_ids_p,
                                optional_ptr<TableFilterSet> filters_p)
      : bind_data(bind_data_p), column_ids(column_ids_p),
        projection_ids(projection_ids_p), filters(filters_p) {}

  optional_ptr<const FunctionData> bind_data;
  const vector<column_t> &column_ids;
  const vector<idx_t> projection_ids;
  optional_ptr<TableFilterSet> filters;
  struct IndexedTableFunctionInitInputRAI input_rai;

  bool CanRemoveFilterColumns() const {
    if (projection_ids.empty()) {
      // Not set, can't remove filter columns
      return false;
    } else if (projection_ids.size() == column_ids.size()) {
      // Filter column is used in remainder of plan, can't remove
      return false;
    } else {
      // Less columns need to be projected out than that we scan
      return true;
    }
  }
};

typedef unique_ptr<GlobalTableFunctionState> (
    *indexed_table_function_init_global_t)(
    ClientContext &context, IndexedTableFunctionInitInput &input);
typedef unique_ptr<LocalTableFunctionState> (
    *indexed_table_function_init_local_t)(
    ExecutionContext &context, IndexedTableFunctionInitInput &input,
    GlobalTableFunctionState *global_state);
typedef void (*indexed_table_function_t)(
    ClientContext &context, TableFunctionInput &data, DataChunk &output,
    IndexedRowGroupCollection *indexed_collection,
    IndexedRowGroupCollection *indexed_local_collection);

class IndexedTableFunction : public SimpleNamedParameterFunction {
public:
  DUCKDB_API
  IndexedTableFunction(
      string name, vector<LogicalType> arguments,
      indexed_table_function_t function, table_function_bind_t bind = nullptr,
      indexed_table_function_init_global_t init_global = nullptr,
      indexed_table_function_init_local_t init_local = nullptr);
  DUCKDB_API
  IndexedTableFunction(
      const vector<LogicalType> &arguments, indexed_table_function_t function,
      table_function_bind_t bind = nullptr,
      indexed_table_function_init_global_t init_global = nullptr,
      indexed_table_function_init_local_t init_local = nullptr);
  DUCKDB_API IndexedTableFunction();

  //! Bind function
  //! This function is used for determining the return type of a table producing
  //! function and returning bind data The returned FunctionData object should
  //! be constant and should not be changed during execution.
  table_function_bind_t bind;
  //! (Optional) Bind replace function
  //! This function is called before the regular bind function. It allows
  //! returning a TableRef will be used to to generate a logical plan that
  //! replaces the LogicalGet of a regularly bound TableFunction. The
  //! BindReplace can also return a nullptr to indicate a regular bind needs to
  //! be performed instead.
  table_function_bind_replace_t bind_replace;
  //! (Optional) global init function
  //! Initialize the global operator state of the function.
  //! The global operator state is used to keep track of the progress in the
  //! table function and is shared between all threads working on the table
  //! function.
  indexed_table_function_init_global_t init_global;
  //! (Optional) local init function
  //! Initialize the local operator state of the function.
  //! The local operator state is used to keep track of the progress in the
  //! table function and is thread-local.
  indexed_table_function_init_local_t init_local;
  //! The main function
  indexed_table_function_t function;
  //! The table in-out function (if this is an in-out function)
  table_in_out_function_t in_out_function;
  //! The table in-out final function (if this is an in-out function)
  table_in_out_function_final_t in_out_function_final;
  //! (Optional) statistics function
  //! Returns the statistics of a specified column
  table_statistics_t statistics;
  //! (Optional) dependency function
  //! Sets up which catalog entries this table function depend on
  table_function_dependency_t dependency;
  //! (Optional) cardinality function
  //! Returns the expected cardinality of this scan
  table_function_cardinality_t cardinality;
  //! (Optional) pushdown a set of arbitrary filter expressions, rather than
  //! only simple comparisons with a constant Any functions remaining in the
  //! expression list will be pushed as a regular filter after the scan
  table_function_pushdown_complex_filter_t pushdown_complex_filter;
  //! (Optional) function for rendering the operator to a string in profiling
  //! output
  table_function_to_string_t to_string;
  //! (Optional) return how much of the table we have scanned up to this point
  //! (% of the data)
  table_function_progress_t table_scan_progress;
  //! (Optional) returns the current batch index of the current scan operator
  table_function_get_batch_index_t get_batch_index;
  //! (Optional) returns the extra batch info, currently only used for the
  //! substrait extension
  table_function_get_bind_info get_batch_info;

  table_function_serialize_t serialize;
  table_function_deserialize_t deserialize;
  bool verify_serialization = true;

  //! Whether or not the table function supports projection pushdown. If not
  //! supported a projection will be added that filters out unused columns.
  bool projection_pushdown;
  //! Whether or not the table function supports filter pushdown. If not
  //! supported a filter will be added that applies the table filter directly.
  bool filter_pushdown;
  //! Whether or not the table function can immediately prune out filter columns
  //! that are unused in the remainder of the query plan, e.g., "SELECT i FROM
  //! tbl WHERE j = 42;" - j does not need to leave the table function at all
  bool filter_prune;
  //! Additional function info, passed to the bind
  shared_ptr<TableFunctionInfo> function_info;

  DUCKDB_API bool Equal(const IndexedTableFunction &rhs) const;
};

} // namespace duckdb
