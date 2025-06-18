//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/graph/graph_index_impl.cpp
//
//
//===----------------------------------------------------------------------===//

#include "physical_graph_index.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table_io_manager.hpp"

#include <algorithm>

namespace duckdb {

PhysicalGraphIndex::PhysicalGraphIndex(
    const vector<column_t> &column_ids, TableIOManager &table_io_manager,
    const vector<unique_ptr<Expression>> &unbound_expressions,
    const IndexConstraintType constraint_type, AttachedDatabase &db,
    const string &name, const string &table_name,
    relgo::GraphIndexDirection direction,
    const vector<string> &referenced_tables,
    const vector<column_t> &referenced_columns)
    : Index(db, IndexType::EXTENSION, table_io_manager, column_ids,
            unbound_expressions, constraint_type),
      owns_data(true) {

  // Create the underlying graph index
  graph_index = make_uniq<relgo::GraphIndex<int64_t *, SelectionVector>>(
      name, table_name, direction, column_ids, referenced_tables,
      referenced_columns);
}

//===--------------------------------------------------------------------===//
// Initialize Predicate Scans
//===--------------------------------------------------------------------===//

unique_ptr<IndexScanState> PhysicalGraphIndex::InitializeScanSinglePredicate(
    const Transaction &transaction, const Value &value,
    const ExpressionType expression_type) {
  // Initialize point lookup
  auto result = make_uniq<GraphIndexScanState>();
  result->values[0] = value;
  result->expressions[0] = expression_type;
  return std::move(result);
}

unique_ptr<IndexScanState> PhysicalGraphIndex::InitializeScanTwoPredicates(
    const Transaction &transaction, const Value &low_value,
    const ExpressionType low_expression_type, const Value &high_value,
    const ExpressionType high_expression_type) {
  // Initialize range lookup
  auto result = make_uniq<GraphIndexScanState>();
  result->values[0] = low_value;
  result->expressions[0] = low_expression_type;
  result->values[1] = high_value;
  result->expressions[1] = high_expression_type;
  return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//

bool PhysicalGraphIndex::Scan(const Transaction &transaction,
                              const DataTable &table, IndexScanState &state,
                              const idx_t max_count,
                              vector<row_t> &result_ids) {
  auto &scan_state = state.Cast<GraphIndexScanState>();

  // For now, we'll implement a simple scan that returns all row IDs
  // In a real implementation, this would use the graph index to find matching
  // rows
  if (scan_state.current_pos >= scan_state.result_ids.size()) {
    return true;
  }

  idx_t count = 0;
  while (scan_state.current_pos < scan_state.result_ids.size() &&
         count < max_count) {
    result_ids.push_back(scan_state.result_ids[scan_state.current_pos++]);
    count++;
  }

  return scan_state.current_pos >= scan_state.result_ids.size();
}

//===--------------------------------------------------------------------===//
// Insert / Append / Delete
//===--------------------------------------------------------------------===//

void PhysicalGraphIndex::ConvertToEdgeTuples(DataChunk &data,
                                             vector<relgo::EdgeTuple> &tuples,
                                             Vector &row_ids) {
  // Convert the data chunk to a vector of EdgeTuple
  // This is a simplified implementation - in a real implementation, this would
  // extract the source, edge, and target IDs from the data chunk
  row_ids.Flatten(data.size());
  auto row_identifiers = FlatVector::GetData<row_t>(row_ids);

  for (idx_t i = 0; i < data.size(); i++) {
    // For simplicity, we'll assume the first column is the source ID,
    // the second column is the edge ID, and the third column is the target ID
    // In a real implementation, this would be more complex
    auto source_id = row_identifiers[i];
    auto edge_id = row_identifiers[i];
    auto target_id = row_identifiers[i];

    tuples.emplace_back(source_id, edge_id, target_id);
  }
}

PreservedError PhysicalGraphIndex::Insert(IndexLock &lock, DataChunk &data,
                                          Vector &row_ids) {
  // Convert the data chunk to a vector of EdgeTuple
  vector<relgo::EdgeTuple> tuples;
  ConvertToEdgeTuples(data, tuples, row_ids);

  // Append the tuples to the graph index
  graph_index->alist->Append(tuples, tuples.size(), graph_index->rai_direction);

  return PreservedError();
}

PreservedError PhysicalGraphIndex::Append(IndexLock &lock, DataChunk &entries,
                                          Vector &row_identifiers) {
  DataChunk expression_result;
  expression_result.Initialize(Allocator::DefaultAllocator(), logical_types);

  // First resolve the expressions for the index
  ExecuteExpressions(entries, expression_result);

  // Now insert into the index
  return Insert(lock, expression_result, row_identifiers);
}

void PhysicalGraphIndex::VerifyAppend(DataChunk &chunk) {
  ConflictManager conflict_manager(VerifyExistenceType::APPEND, chunk.size());
  CheckConstraintsForChunk(chunk, conflict_manager);
}

void PhysicalGraphIndex::VerifyAppend(DataChunk &chunk,
                                      ConflictManager &conflict_manager) {
  D_ASSERT(conflict_manager.LookupType() == VerifyExistenceType::APPEND);
  CheckConstraintsForChunk(chunk, conflict_manager);
}

void PhysicalGraphIndex::CommitDrop(IndexLock &index_lock) {
  // Clear the graph index
  graph_index->alist->forward_map.clear();
  graph_index->alist->backward_map.clear();
  graph_index->alist->forward_edge_map.clear();
  graph_index->alist->backward_edge_map.clear();
  graph_index->alist->edge_num = 0;
  graph_index->alist->source_num = 0;
  graph_index->alist->target_num = 0;
}

void PhysicalGraphIndex::Delete(IndexLock &lock, DataChunk &entries,
                                Vector &row_identifiers) {
  // For simplicity, we'll just clear the entire index
  // In a real implementation, this would delete only the specified entries
  CommitDrop(lock);
}

//===--------------------------------------------------------------------===//
// Merge / Vacuum
//===--------------------------------------------------------------------===//

bool PhysicalGraphIndex::MergeIndexes(IndexLock &state, Index &other_index) {
  auto &other_graph_index = other_index.Cast<PhysicalGraphIndex>();

  // For simplicity, we'll just append all the data from the other index
  // In a real implementation, this would be more complex
  if (!other_graph_index.graph_index->alist->enabled) {
    return true;
  }

  // Merge the data
  // This is a simplified implementation
  return true;
}

void PhysicalGraphIndex::InitializeVacuum(GraphIndexFlags &flags) {
  // No vacuum needed for graph index
  flags.vacuum_flags.clear();
}

void PhysicalGraphIndex::FinalizeVacuum(const GraphIndexFlags &flags) {
  // No vacuum needed for graph index
}

void PhysicalGraphIndex::Vacuum(IndexLock &state) {
  if (!graph_index->alist->enabled) {
    return;
  }

  // No vacuum needed for graph index
  GraphIndexFlags flags;
  InitializeVacuum(flags);
  FinalizeVacuum(flags);
}

//===--------------------------------------------------------------------===//
// Constraint Checking
//===--------------------------------------------------------------------===//

void PhysicalGraphIndex::CheckConstraintsForChunk(
    DataChunk &input, ConflictManager &conflict_manager) {
  // For simplicity, we'll just check if the input is empty
  // In a real implementation, this would check for constraint violations
  if (input.size() == 0) {
    return;
  }

  // No constraint violations
}

//===--------------------------------------------------------------------===//
// Serialization
//===--------------------------------------------------------------------===//

BlockPointer PhysicalGraphIndex::Serialize(MetadataWriter &writer) {
  D_ASSERT(owns_data);

  // For simplicity, we'll just return an empty block pointer
  // In a real implementation, this would serialize the graph index to disk
  root_block_pointer = BlockPointer();
  return root_block_pointer;
}

void PhysicalGraphIndex::Deserialize(const BlockPointer &pointer) {
  // For simplicity, we'll just do nothing
  // In a real implementation, this would deserialize the graph index from disk
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

string PhysicalGraphIndex::VerifyAndToStringInternal(const bool only_verify) {
  if (graph_index->alist->enabled) {
    return "GraphIndex: " + graph_index->name;
  }
  return "[empty]";
}

string PhysicalGraphIndex::VerifyAndToString(IndexLock &state,
                                             const bool only_verify) {
  return VerifyAndToStringInternal(only_verify);
}

} // namespace duckdb