//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/append_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/transaction/transaction_data.hpp"

namespace duckdb {
class ColumnSegment;
class DataTable;
class IndexedLocalTableStorage;
class IndexedRowGroup;
class UpdateSegment;

struct IndexedTableAppendState;

struct IndexedRowGroupAppendState {
  IndexedRowGroupAppendState(IndexedTableAppendState &parent_p)
      : parent(parent_p) {}

  //! The parent append state
  IndexedTableAppendState &parent;
  //! The current row_group we are appending to
  IndexedRowGroup *row_group;
  //! The column append states
  unsafe_unique_array<ColumnAppendState> states;
  //! Offset within the row_group
  idx_t offset_in_row_group;
};

struct IndexedTableAppendState {
  IndexedTableAppendState();
  ~IndexedTableAppendState();

  IndexedRowGroupAppendState row_group_append_state;
  unique_lock<mutex> append_lock;
  row_t row_start;
  row_t current_row;
  //! The total number of rows appended by the append operation
  idx_t total_append_count;
  //! The first row-group that has been appended to
  IndexedRowGroup *start_row_group;
  //! The transaction data
  TransactionData transaction;
  //! The remaining append count, only if the append count is known beforehand
  idx_t remaining;
};

struct IndexedLocalAppendState {
  IndexedTableAppendState append_state;
  IndexedLocalTableStorage *storage;
};

} // namespace duckdb
