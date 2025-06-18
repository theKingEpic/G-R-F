//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/parsed_data/bound_create_rai_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "bound_create_info.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "logical_create_rai.hpp"

namespace duckdb {

struct BoundCreateRAIInfo : public BoundCreateInfo {
	BoundCreateRAIInfo(unique_ptr<CreateInfo> base) : BoundCreateInfo(move(base)) {
	}

	string name;
	unique_ptr<BoundTableRef> table;
	relgo::GraphIndexDirection rai_direction;
	std::vector<unique_ptr<BoundTableRef>> referenced_tables;
	std::vector<unique_ptr<Expression>> columns;
	std::vector<unique_ptr<Expression>> references;
	std::vector<column_t> base_column_ids;
	std::vector<column_t> referenced_column_ids;
	unique_ptr<LogicalCreateRAI> plan;
};
} // namespace duckdb
