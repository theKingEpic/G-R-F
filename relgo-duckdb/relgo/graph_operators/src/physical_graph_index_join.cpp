#include "../includes/physical_graph_index_join.hpp"

#include "../graph_operators/includes/physical_indexed_table_scan.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

namespace duckdb {

PhysicalGraphIndexJoin::PhysicalGraphIndexJoin(vector<IndexedJoinCondition> &conditions_p) {
	conditions.resize(conditions_p.size());
	// we reorder conditions so the ones with COMPARE_EQUAL occur first
	idx_t equal_position = 0;
	idx_t other_position = conditions_p.size() - 1;
	for (idx_t i = 0; i < conditions_p.size(); i++) {
		if (conditions_p[i].comparison == ExpressionType::COMPARE_EQUAL ||
		    conditions_p[i].comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			// COMPARE_EQUAL and COMPARE_NOT_DISTINCT_FROM, move to the start
			conditions[equal_position++] = std::move(conditions_p[i]);
		} else {
			// other expression, move to the end
			conditions[other_position--] = std::move(conditions_p[i]);
		}
	}
}

bool PhysicalGraphIndexJoin::PushdownZoneFilter(PhysicalOperator *op, idx_t table_index,
                                                const shared_ptr<relgo::bitmask_vector> &zone_filter,
                                                const shared_ptr<relgo::bitmask_vector> &zone_sel) const {
	if (table_index == 0) {
		return false;
	}

	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		auto scan_op = dynamic_cast<PhysicalIndexedTableScan *>(op);
		if (scan_op->table_index == table_index) {
			if (scan_op->row_bitmask) {
				auto &result_row_bitmask = *scan_op->row_bitmask;
				auto &input_row_bitmask = *zone_filter;
				auto &result_zone_bitmask = *scan_op->zone_bitmask;
				auto &input_zone_bitmask = *zone_sel;
				for (idx_t i = 0; i < zone_sel->size(); i++) {
					result_zone_bitmask[i] = result_zone_bitmask[i] & input_zone_bitmask[i];
				}
				auto zone_filter_index = 0;
				for (idx_t i = 0; i < result_zone_bitmask.size(); i++) {
					auto zone_count = result_zone_bitmask[i] * STANDARD_VECTOR_SIZE;
					for (idx_t j = 0; j < zone_count; j++) {
						auto current_index = i * STANDARD_VECTOR_SIZE + j;
						result_row_bitmask[current_index] =
						    result_row_bitmask[current_index] & input_row_bitmask[current_index];
					}
					zone_filter_index += STANDARD_VECTOR_SIZE;
				}
			} else {
				scan_op->row_bitmask = zone_filter;
				scan_op->zone_bitmask = zone_sel;
			}
			return true;
		}
		return false;
	} else {
		if (table_index == 0) {
			return false;
		}
		for (auto &child : op->children) {
			if (PushdownZoneFilter(child.get(), table_index, zone_filter, zone_sel)) {
				return true;
			}
		}
		return false;
	}

	return false;
}

void PhysicalGraphIndexJoin::ConstructEmptyJoinResult(JoinType join_type, bool has_null, DataChunk &input,
                                                      DataChunk &result) {
	// empty hash table, special case
	if (join_type == JoinType::ANTI) {
		// anti join with empty hash table, NOP join
		// return the input
		D_ASSERT(input.ColumnCount() == result.ColumnCount());
		result.Reference(input);
	} else if (join_type == JoinType::MARK) {
		// MARK join with empty hash table
		D_ASSERT(join_type == JoinType::MARK);
		D_ASSERT(result.ColumnCount() == input.ColumnCount() + 1);
		auto &result_vector = result.data.back();
		D_ASSERT(result_vector.GetType() == LogicalType::BOOLEAN);
		// for every data vector, we just reference the child chunk
		result.SetCardinality(input);
		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			result.data[i].Reference(input.data[i]);
		}
		// for the MARK vector:
		// if the HT has no NULL values (i.e. empty result set), return a vector that has false for every input
		// entry if the HT has NULL values (i.e. result set had values, but all were NULL), return a vector that
		// has NULL for every input entry
		if (!has_null) {
			auto bool_result = FlatVector::GetData<bool>(result_vector);
			for (idx_t i = 0; i < result.size(); i++) {
				bool_result[i] = false;
			}
		} else {
			FlatVector::Validity(result_vector).SetAllInvalid(result.size());
		}
	} else if (join_type == JoinType::LEFT || join_type == JoinType::OUTER || join_type == JoinType::SINGLE) {
		// LEFT/FULL OUTER/SINGLE join and build side is empty
		// for the LHS we reference the data
		result.SetCardinality(input.size());
		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			result.data[i].Reference(input.data[i]);
		}
		// for the RHS
		for (idx_t k = input.ColumnCount(); k < result.ColumnCount(); k++) {
			result.data[k].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result.data[k], true);
		}
	}
}

} // namespace duckdb