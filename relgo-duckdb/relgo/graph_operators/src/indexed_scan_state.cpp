#include "../includes/indexed_scan_state.hpp"

#include "../includes/indexed_row_group.hpp"
#include "../includes/indexed_row_group_collection.hpp"
#include "../includes/indexed_row_group_segment_tree.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

const vector<storage_t> &IndexedCollectionScanState::GetColumnIds() {
	return parent.GetColumnIds();
}

TableFilterSet *IndexedCollectionScanState::GetFilters() {
	return parent.GetFilters();
}

AdaptiveFilter *IndexedCollectionScanState::GetAdaptiveFilter() {
	return parent.GetAdaptiveFilter();
}

IndexedCollectionScanState::IndexedCollectionScanState(IndexedTableScanState &parent_p)
    : row_group(nullptr), vector_index(0), max_row_group_row(0), row_groups(nullptr), max_row(0), batch_index(0),
      parent(parent_p) {
}

bool IndexedCollectionScanState::Scan(DuckTransaction &transaction, DataChunk &result) {
	while (row_group) {
		row_group->Scan(transaction, *this, result);
		if (result.size() > 0) {
			return true;
		} else if (max_row <= row_group->start + row_group->count) {
			row_group = nullptr;
			return false;
		} else {
			do {
				row_group = row_groups->GetNextSegment(row_group);
				if (row_group) {
					if (row_group->start >= max_row) {
						row_group = nullptr;
						break;
					}
					bool scan_row_group = row_group->InitializeScan(*this);
					if (scan_row_group) {
						// scan this row group
						break;
					}
				}
			} while (row_group);
		}
	}
	return false;
}

bool IndexedCollectionScanState::ScanCommitted(DataChunk &result, SegmentLock &l, TableScanType type) {
	while (row_group) {
		row_group->ScanCommitted(*this, result, type);
		if (result.size() > 0) {
			return true;
		} else {
			row_group = row_groups->GetNextSegment(l, row_group);
			if (row_group) {
				row_group->InitializeScan(*this);
			}
		}
	}
	return false;
}

bool IndexedCollectionScanState::ScanCommitted(DataChunk &result, TableScanType type) {
	while (row_group) {
		row_group->ScanCommitted(*this, result, type);
		if (result.size() > 0) {
			return true;
		} else {
			row_group = row_groups->GetNextSegment(row_group);
			if (row_group) {
				row_group->InitializeScan(*this);
			}
		}
	}
	return false;
}

} // namespace duckdb
