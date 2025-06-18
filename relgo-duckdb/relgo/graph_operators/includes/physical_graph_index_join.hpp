//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_merge_sip_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "../includes/indexed_join_condition.hpp"
#include "../includes/sip_hashtable.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

//! PhysicalHashJoin represents a hash loop join between two tables
class PhysicalGraphIndexJoin {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INDEX_JOIN;

public:
	vector<IndexedJoinCondition> conditions;

	PhysicalGraphIndexJoin(vector<IndexedJoinCondition> &conditions_p);

	bool PushdownZoneFilter(PhysicalOperator *op, idx_t table_index,
	                        const shared_ptr<relgo::bitmask_vector> &zone_filter,
	                        const shared_ptr<relgo::bitmask_vector> &zone_sel) const;

public:
	//! Construct the join result of a join with an empty RHS
	static void ConstructEmptyJoinResult(JoinType type, bool has_null, DataChunk &input, DataChunk &result);
	//! Construct the remainder of a Full Outer Join based on which tuples in the RHS found no match
	static void ConstructFullOuterJoinResult(bool *found_match, ColumnDataCollection &input, DataChunk &result,
	                                         ColumnDataScanState &scan_state);
};

} // namespace duckdb
