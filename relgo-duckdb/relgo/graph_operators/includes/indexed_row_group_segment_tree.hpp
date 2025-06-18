//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_group_segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/segment_tree.hpp"
#include "indexed_row_group.hpp"

namespace duckdb {
struct DataTableInfo;
class PersistentTableData;
class MetadataReader;

class IndexedRowGroupSegmentTree : public SegmentTree<IndexedRowGroup, true> {
public:
  IndexedRowGroupSegmentTree(IndexedRowGroupCollection &collection);
  ~IndexedRowGroupSegmentTree() override;

  void Initialize(PersistentTableData &data);

protected:
  unique_ptr<IndexedRowGroup> IndexedLoadSegment();

  IndexedRowGroupCollection &collection;
  idx_t current_row_group;
  idx_t max_row_group;
  unique_ptr<MetadataReader> reader;
};

} // namespace duckdb
