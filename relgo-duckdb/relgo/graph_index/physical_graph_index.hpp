//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/graph/physical_graph_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "alist.hpp"
#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "graph_index.hpp"

namespace duckdb {

// Forward declarations
struct GraphIndexScanState;
struct GraphIndexFlags {
  vector<bool> vacuum_flags;
  vector<idx_t> merge_buffer_counts;
};

//! GraphIndex implementation that inherits from the Index base class
class PhysicalGraphIndex : public Index {
public:
  //! Constructs a GraphIndex
  PhysicalGraphIndex(const vector<column_t> &column_ids,
                     TableIOManager &table_io_manager,
                     const vector<unique_ptr<Expression>> &unbound_expressions,
                     const IndexConstraintType constraint_type,
                     AttachedDatabase &db, const string &name = "graph_index",
                     const string &table_name = "graph_table",
                     relgo::GraphIndexDirection direction =
                         relgo::GraphIndexDirection::DIRECTED,
                     const vector<string> &referenced_tables = {},
                     const vector<column_t> &referenced_columns = {});

  //! The underlying graph index implementation
  unique_ptr<relgo::GraphIndex<int64_t *, SelectionVector>> graph_index;
  //! True if the index owns its data
  bool owns_data;

public:
  //! Initialize a single predicate scan on the index with the given expression
  //! and column IDs
  unique_ptr<IndexScanState>
  InitializeScanSinglePredicate(const Transaction &transaction,
                                const Value &value,
                                const ExpressionType expression_type) override;
  //! Initialize a two predicate scan on the index with the given expression and
  //! column IDs
  unique_ptr<IndexScanState> InitializeScanTwoPredicates(
      const Transaction &transaction, const Value &low_value,
      const ExpressionType low_expression_type, const Value &high_value,
      const ExpressionType high_expression_type) override;
  //! Performs a lookup on the index, fetching up to max_count result IDs.
  //! Returns true if all row IDs were fetched, and false otherwise
  bool Scan(const Transaction &transaction, const DataTable &table,
            IndexScanState &state, const idx_t max_count,
            vector<row_t> &result_ids) override;

  //! Called when data is appended to the index. The lock obtained from
  //! InitializeLock must be held
  PreservedError Append(IndexLock &lock, DataChunk &entries,
                        Vector &row_identifiers) override;
  //! Verify that data can be appended to the index without a constraint
  //! violation
  void VerifyAppend(DataChunk &chunk) override;
  //! Verify that data can be appended to the index without a constraint
  //! violation using the conflict manager
  void VerifyAppend(DataChunk &chunk,
                    ConflictManager &conflict_manager) override;
  //! Deletes all data from the index. The lock obtained from InitializeLock
  //! must be held
  void CommitDrop(IndexLock &index_lock) override;
  //! Delete a chunk of entries from the index. The lock obtained from
  //! InitializeLock must be held
  void Delete(IndexLock &lock, DataChunk &entries,
              Vector &row_identifiers) override;
  //! Insert a chunk of entries into the index
  PreservedError Insert(IndexLock &lock, DataChunk &data,
                        Vector &row_ids) override;

  //! Merge another index into this index. The lock obtained from InitializeLock
  //! must be held, and the other index must also be locked during the merge
  bool MergeIndexes(IndexLock &state, Index &other_index) override;

  //! Traverses the index and vacuums the qualifying nodes. The lock obtained
  //! from InitializeLock must be held
  void Vacuum(IndexLock &state) override;

  //! Performs constraint checking for a chunk of input data
  void CheckConstraintsForChunk(DataChunk &input,
                                ConflictManager &conflict_manager) override;

  //! Returns the string representation of the index, or only traverses and
  //! verifies the index
  string VerifyAndToString(IndexLock &state, const bool only_verify) override;

  //! Serializes the index and returns the pair of block_id offset positions
  BlockPointer Serialize(MetadataWriter &writer) override;

private:
  //! Initialize a merge operation
  void InitializeMerge(GraphIndexFlags &flags);

  //! Initialize a vacuum operation
  void InitializeVacuum(GraphIndexFlags &flags);
  //! Finalize a vacuum operation
  void FinalizeVacuum(const GraphIndexFlags &flags);

  //! Internal function to return the string representation of the index
  string VerifyAndToStringInternal(const bool only_verify);

  //! Deserialize the index from disk
  void Deserialize(const BlockPointer &pointer);

  //! Convert a DataChunk to a vector of EdgeTuple
  void ConvertToEdgeTuples(DataChunk &data, vector<relgo::EdgeTuple> &tuples,
                           Vector &row_ids);
};

//! Scan state for the GraphIndex
struct GraphIndexScanState : public IndexScanState {
  //! Scan predicates (single predicate scan or range scan)
  Value values[2];
  //! Expressions of the scan predicates
  ExpressionType expressions[2];
  bool checked = false;
  //! All scanned row IDs
  vector<row_t> result_ids;
  //! Current position in the scan
  idx_t current_pos = 0;
};

} // namespace duckdb
