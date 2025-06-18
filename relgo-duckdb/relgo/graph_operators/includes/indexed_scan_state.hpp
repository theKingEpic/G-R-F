//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/scan_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "../../utils/types.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/scan_options.hpp"
#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/segment_lock.hpp"

namespace duckdb {
class ColumnSegment;
class LocalTableStorage;
class IndexedCollectionScanState;
class CollectionScanState;
class Index;
class IndexedRowGroup;
class IndexedRowGroupCollection;
class UpdateSegment;
class IndexedTableScanState;
class ColumnSegment;
class ColumnSegmentTree;
class ValiditySegment;
class TableFilterSet;
class ColumnData;
class DuckTransaction;
class IndexedRowGroupSegmentTree;

class IndexedCollectionScanState {
public:
	IndexedCollectionScanState(IndexedTableScanState &parent_p);

	//! The current row_group we are scanning
	IndexedRowGroup *row_group;
	//! The vector index within the row_group
	idx_t vector_index;
	//! The maximum row within the row group
	idx_t max_row_group_row;
	//! Child column scans
	unsafe_unique_array<ColumnScanState> column_scans;
	//! Row group segment tree
	IndexedRowGroupSegmentTree *row_groups;
	//! The total maximum row index
	idx_t max_row;
	//! The current batch index
	idx_t batch_index;

public:
	void Initialize(const vector<LogicalType> &types);
	const vector<storage_t> &GetColumnIds();
	TableFilterSet *GetFilters();
	AdaptiveFilter *GetAdaptiveFilter();
	bool Scan(DuckTransaction &transaction, DataChunk &result);
	bool ScanCommitted(DataChunk &result, TableScanType type);
	bool ScanCommitted(DataChunk &result, SegmentLock &l, TableScanType type);

	// private:
	IndexedTableScanState &parent;
};

class IndexedTableScanState : public TableScanState {
public:
	IndexedTableScanState()
	    : table_state(*this), local_state(*this), table_filters(nullptr), rowids(nullptr), zones(nullptr),
	      zones_sel(nullptr), storage_info(nullptr) {};

	//! The underlying table scan state
	IndexedCollectionScanState table_state;
	//! Transaction-local scan state
	IndexedCollectionScanState local_state;

	shared_ptr<relgo::rows_vector> rowids;
	shared_ptr<relgo::bitmask_vector> zones;
	shared_ptr<relgo::bitmask_vector> zones_sel;
	idx_t rows_offset = 0;
	row_t rows_count = -1;
	shared_ptr<DataTableInfo> storage_info;

public:
	//! The column identifiers of the scan
	vector<storage_t> column_ids;
	//! The table filters (if any)
	TableFilterSet *table_filters;
	//! Adaptive filter info (if any)
	unique_ptr<AdaptiveFilter> adaptive_filter;
};

class IndexedCreateIndexScanState : public IndexedTableScanState {
public:
	vector<unique_ptr<StorageLockKey>> locks;
	unique_lock<mutex> append_lock;
	SegmentLock segment_lock;
};

struct IndexedParallelCollectionScanState {
	IndexedParallelCollectionScanState() : collection(nullptr), current_row_group(nullptr), processed_rows(0) {};

	//! The row group collection we are scanning
	IndexedRowGroupCollection *collection;
	IndexedRowGroup *current_row_group;
	idx_t vector_index;
	idx_t max_row;
	idx_t batch_index;
	atomic<idx_t> processed_rows;
	mutex lock;
};

struct IndexedParallelTableScanState {
	//! Parallel scan state for the table
	IndexedParallelCollectionScanState scan_state;
	//! Parallel scan state for the transaction-local state
	IndexedParallelCollectionScanState local_state;
};

} // namespace duckdb
