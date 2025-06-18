
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/joinside.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "../../graph_index/graph_index.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! JoinCondition represents a left-right comparison join condition
struct IndexedJoinCondition : public JoinCondition {
public:
IndexedJoinCondition():JoinCondition() {
	}

public:
vector<unique_ptr<relgo::GraphIndexInfo<int64_t *, SelectionVector>>> rais;
};

}

