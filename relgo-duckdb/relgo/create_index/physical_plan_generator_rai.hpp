//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_plan_generator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_tokens.hpp"
#include "duckdb/planner/operator/logical_limit_percent.hpp"
#include "logical_create_rai.hpp"

namespace duckdb {
class ClientContext;
class ColumnDataCollection;

//! The physical plan generator generates a physical execution plan from a
//! logical query plan
class PhysicalPlanGeneratorRAI : public PhysicalPlanGenerator {
public:
	explicit PhysicalPlanGeneratorRAI(ClientContext &context);
	~PhysicalPlanGeneratorRAI();

	unique_ptr<PhysicalOperator> CreatePlan(LogicalCreateRAI &op);
};
} // namespace duckdb
