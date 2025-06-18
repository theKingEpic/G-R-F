//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_indexed_table_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "../includes/indexed_row_group_collection.hpp"
#include "../includes/indexed_table_function.hpp"
#include "duckdb/common/extra_operator_info.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

class PhysicalIndexedTableScan : public PhysicalOperator {
public:
  static constexpr const PhysicalOperatorType TYPE =
      PhysicalOperatorType::TABLE_SCAN;

public:
  //! Table scan that immediately projects out filter columns that are unused in
  //! the remainder of the query plan
  PhysicalIndexedTableScan(
      vector<LogicalType> types, IndexedTableFunction function,
      idx_t table_index, unique_ptr<FunctionData> bind_data,
      vector<LogicalType> returned_types, vector<column_t> column_ids,
      vector<unique_ptr<Expression>> filter, vector<idx_t> projection_ids,
      vector<string> names, unique_ptr<TableFilterSet> table_filters,
      idx_t estimated_cardinality, ExtraOperatorInfo extra_info);

  //! The table id referenced in logical plan
  idx_t table_index;
  //! The table function
  IndexedTableFunction function;
  //! Bind data of the function
  unique_ptr<FunctionData> bind_data;
  //! The types of ALL columns that can be returned by the table function
  vector<LogicalType> returned_types;
  //! The column ids used within the table function
  vector<column_t> column_ids;
  //! The projected-out column ids
  vector<idx_t> projection_ids;
  //! The names of the columns
  vector<string> names;
  //! The table filters
  unique_ptr<TableFilterSet> table_filters;
  //! Currently stores any filters applied to file names (as strings)
  ExtraOperatorInfo extra_info;
  //! Expression
  unique_ptr<Expression> expression;

  mutex parallel_lock;

public:
  string GetName() const override;
  string ParamsToString() const override;

  bool Equals(const PhysicalOperator &other) const override;

public:
  unique_ptr<LocalSourceState>
  GetLocalSourceState(ExecutionContext &context,
                      GlobalSourceState &gstate) const override;
  unique_ptr<GlobalSourceState>
  GetGlobalSourceState(ClientContext &context) const override;
  SourceResultType GetData(ExecutionContext &context, DataChunk &chunk,
                           OperatorSourceInput &input) const override;
  idx_t GetBatchIndex(ExecutionContext &context, DataChunk &chunk,
                      GlobalSourceState &gstate,
                      LocalSourceState &lstate) const override;

  bool IsSource() const override { return true; }
  bool ParallelSource() const override { return true; }

  bool SupportsBatchIndex() const override {
    return function.get_batch_index != nullptr;
  }

  double GetProgress(ClientContext &context,
                     GlobalSourceState &gstate) const override;

  // substrait::Rel* ToSubstraitClass(unordered_map<int, string>& tableid2name)
  // const override;

public:
  //! The rows filter
  shared_ptr<relgo::rows_vector> rows_filter;
  shared_ptr<relgo::bitmask_vector> row_bitmask;
  shared_ptr<relgo::bitmask_vector> zone_bitmask;
  row_t rows_count;
  shared_ptr<IndexedRowGroupCollection> indexed_collection;
};

} // namespace duckdb
