#ifndef GRAPH_INDEX_BUILDER_HPP
#define GRAPH_INDEX_BUILDER_HPP

#include "../graph_index/alist.hpp"
#include "../graph_index/graph_index.hpp"
#include "bound_create_rai_info.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "logical_create_rai.hpp"

namespace relgo {

class ClientContext;

class GraphIndexBuildInfo {
public:
	GraphIndexBuildInfo(std::string _index_name, std::string _rel_name, std::string _from_col, std::string _to_col,
	                    std::string _from_ref_table_name, std::string _from_ref_col, std::string _to_ref_table_name,
	                    std::string _to_ref_col, GraphIndexDirection _direction)
	    : index_name(_index_name), rel_name(_rel_name), from_col(_from_col), to_col(_to_col),
	      from_ref_table_name(_from_ref_table_name), from_ref_col(_from_ref_col), to_ref_table_name(_to_ref_table_name),
	      to_ref_col(_to_ref_col), direction(_direction), rel_schema_name("") {
	}
	GraphIndexBuildInfo(std::string create_statement);
	GraphIndexBuildInfo() {
	}

	GraphIndexDirection direction;

	std::string index_name;
	std::string rel_schema_name;

	std::string rel_name;
	std::string from_ref_table_name;
	std::string to_ref_table_name;

	std::string from_col;
	std::string to_col;
	std::string from_ref_col;
	std::string to_ref_col;
};

class GraphIndexBuilder {
public:
	GraphIndexBuilder(duckdb::ClientContext &_context) : context(_context) {
	}

	duckdb::unique_ptr<duckdb::CreateStatement> transformCreateRAI(GraphIndexBuildInfo &info);
	duckdb::unique_ptr<duckdb::BoundCreateRAIInfo> BindCreateRAIInfo(duckdb::Binder &input_binder,
	                                                                 duckdb::unique_ptr<duckdb::CreateInfo> info);

	duckdb::unique_ptr<duckdb::LogicalCreateRAI> CreateLogicalPlan(duckdb::BoundCreateRAIInfo &bound_info);

	duckdb::unique_ptr<duckdb::PhysicalOperator> generateGraphIndexPlan(GraphIndexBuildInfo &info);

	duckdb::ClientContext &context;
};

} // namespace relgo

#endif