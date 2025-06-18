#include "graph_index_builder.hpp"

#include "bound_create_rai_info.hpp"
#include "create_rai_info.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "logical_create_rai.hpp"
#include "physical_create_rai.hpp"
#include "physical_plan_generator_rai.hpp"
#include "rai_binder.hpp"

namespace relgo {

GraphIndexBuildInfo::GraphIndexBuildInfo(std::string create_statement) {
	std::vector<std::string> splited_str;
	std::string token;

	for (char ch : create_statement) {
		if (std::isspace(ch) || ch == ',' || ch == '.' || ch == '(' || ch == ')') {
			if (!token.empty()) {
				splited_str.push_back(token);
				token.clear();
			}
		} else {
			token += ch;
		}
	}

	if (!token.empty()) {
		splited_str.push_back(token);
	}

	index_name = splited_str[3];
	rel_name = splited_str[5];
	from_col = splited_str[7];
	to_col = splited_str[12];
	from_ref_table_name = splited_str[9];
	from_ref_col = splited_str[10];
	to_ref_table_name = splited_str[14];
	to_ref_col = splited_str[15];

	if (splited_str[1] == "SELF")
		direction = relgo::GraphIndexDirection::SELF;
	else if (splited_str[1] == "UNDIRECTED")
		direction = relgo::GraphIndexDirection::UNDIRECTED;
	else if (splited_str[1] == "PKFK")
		direction = relgo::GraphIndexDirection::PKFK;
	else if (splited_str[1] == "DIRECTED")
		direction = relgo::GraphIndexDirection::DIRECTED;
	else
		direction = relgo::GraphIndexDirection::UNDIRECTED;
}

duckdb::unique_ptr<duckdb::CreateStatement> GraphIndexBuilder::transformCreateRAI(GraphIndexBuildInfo &input_info) {
	auto result = duckdb::make_uniq<duckdb::CreateStatement>();
	auto info = duckdb::make_uniq<duckdb::CreateRAIInfo>();

	info->name = input_info.index_name;
	auto tableref = duckdb::make_uniq<duckdb::BaseTableRef>();
	tableref->table_name = input_info.rel_name;
	if (!input_info.rel_schema_name.empty()) {
		tableref->schema_name = input_info.rel_schema_name;
	}
	info->table = move(tableref);
	info->direction = input_info.direction;

	auto fromref = duckdb::make_uniq<duckdb::BaseTableRef>();
	fromref->table_name = input_info.from_ref_table_name;
	auto toref = duckdb::make_uniq<duckdb::BaseTableRef>();
	toref->table_name = input_info.to_ref_table_name;

	fromref->alias = fromref->table_name;
	toref->alias = toref->table_name;
	if (fromref->table_name == toref->table_name) {
		fromref->alias = fromref->table_name + "_from";
		toref->alias = toref->table_name + "_to";
	} else {
		if (fromref->table_name == input_info.rel_name) {
			fromref->alias = fromref->table_name + "_from";
		}
		if (toref->table_name == input_info.rel_name) {
			toref->alias = toref->table_name + "_to";
		}
	}

	info->columns.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>(input_info.from_col, input_info.rel_name));
	info->references.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>(input_info.from_ref_col, fromref->alias));
	info->referenced_tables.push_back(move(fromref));
	info->columns.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>(input_info.to_col, input_info.rel_name));
	info->references.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>(input_info.to_ref_col, toref->alias));
	info->referenced_tables.push_back(move(toref));

	result->info = move(info);

	return result;
}

duckdb::unique_ptr<duckdb::BoundCreateRAIInfo>
GraphIndexBuilder::BindCreateRAIInfo(duckdb::Binder &input_binder, duckdb::unique_ptr<duckdb::CreateInfo> info) {
	auto &base = (duckdb::CreateRAIInfo &)*info;
	auto result = duckdb::make_uniq<duckdb::BoundCreateRAIInfo>(move(info));

	result->name = base.name;
	result->table = input_binder.Bind(*base.table);
	result->rai_direction = base.direction;
	for (auto &ref : base.referenced_tables) {
		result->referenced_tables.push_back(move(input_binder.Bind(*ref)));
	}

	duckdb::RAIBinder binder(input_binder, input_binder.context);
	for (auto &expr : base.columns) {
		result->columns.push_back(move(binder.Bind(expr)));
	}
	for (auto &expr : base.references) {
		result->references.push_back(move(binder.Bind(expr)));
	}

	auto plan = CreateLogicalPlan(*result);
	result->plan = move(plan);

	return result;
}

static duckdb::unique_ptr<duckdb::LogicalCreateRAI> CreatePlanForPKFKRAI(duckdb::BoundCreateRAIInfo &bound_info) {
	duckdb::vector<column_t> referenced_column_ids;
	duckdb::vector<column_t> base_column_ids;
	duckdb::vector<duckdb::TableCatalogEntry *> referenced_tables;

	auto from_tbl_ref = reinterpret_cast<duckdb::BoundBaseTableRef *>(bound_info.referenced_tables[0].get());
	auto base_tbl_ref = reinterpret_cast<duckdb::BoundBaseTableRef *>(bound_info.table.get());
	auto from_get = reinterpret_cast<duckdb::LogicalGet *>(from_tbl_ref->get.get());
	auto base_get = reinterpret_cast<duckdb::LogicalGet *>(base_tbl_ref->get.get());
	from_get->column_ids.push_back((column_t)-1);
	base_get->column_ids.push_back((column_t)-1);

	std::string col_name = bound_info.references[0]->GetName();
	const duckdb::ColumnList &entry = from_get->GetTable()->GetColumns();
	if (entry.ColumnExists(col_name)) {
		referenced_column_ids.push_back(entry.GetColumnIndex(col_name).index);
		referenced_column_ids.push_back(entry.GetColumnIndex(col_name).index);
	} else {
		throw duckdb::Exception("Column " + col_name + " in rai not found");
	}
	col_name = bound_info.references[1]->GetName();
	for (auto &col : bound_info.columns) {
		const duckdb::ColumnList &entry = base_get->GetTable()->GetColumns();
		if (entry.ColumnExists(col->GetName())) {
			std::string col_name = col->GetName();
			base_column_ids.push_back(entry.GetColumnIndex(col_name).index);
		} else {
			throw duckdb::Exception("Column " + col->GetName() + " in rai not found");
		}
	}

	referenced_tables.push_back(from_get->GetTable().get());
	referenced_tables.push_back(base_get->GetTable().get());

	duckdb::LogicalGet *get_ref_from_tbl = (duckdb::LogicalGet *)from_tbl_ref->get.get();
	duckdb::LogicalGet *get_base_tbl = (duckdb::LogicalGet *)base_tbl_ref->get.get();

	int ref_from_tbl_index = get_ref_from_tbl->table_index;
	int base_tbl_index = get_base_tbl->table_index;

	auto join = duckdb::make_uniq<duckdb::LogicalComparisonJoin>(duckdb::JoinType::LEFT);
	join->AddChild(move(base_tbl_ref->get));
	join->AddChild(move(from_tbl_ref->get));
	duckdb::JoinCondition join_condition;
	join_condition.comparison = duckdb::ExpressionType::COMPARE_EQUAL;
	join_condition.left = move(bound_info.columns[0]);
	join_condition.right = move(bound_info.references[0]);
	join->conditions.push_back(move(join_condition));

	duckdb::ColumnBinding proj_0(ref_from_tbl_index, 1);
	duckdb::ColumnBinding proj_1(base_tbl_index, 1);
	duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> selection_list;
	selection_list.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(duckdb::LogicalType::BIGINT, proj_0));
	selection_list.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(duckdb::LogicalType::BIGINT, proj_1));
	auto projection = duckdb::make_uniq<duckdb::LogicalProjection>(0, move(selection_list));
	projection->AddChild(move(join));

	duckdb::ColumnBinding order_0(0, 0);
	duckdb::ColumnBinding order_1(0, 1);
	duckdb::BoundOrderByNode order_0_node(
	    duckdb::OrderType::ASCENDING, duckdb::OrderByNullType::ORDER_DEFAULT,
	    duckdb::make_uniq<duckdb::BoundColumnRefExpression>(duckdb::LogicalType::BIGINT, order_0));
	duckdb::BoundOrderByNode order_1_node(
	    duckdb::OrderType::ASCENDING, duckdb::OrderByNullType::ORDER_DEFAULT,
	    duckdb::make_uniq<duckdb::BoundColumnRefExpression>(duckdb::LogicalType::BIGINT, order_1));
	duckdb::vector<duckdb::BoundOrderByNode> orders;
	orders.push_back(move(order_1_node));
	auto order_by = duckdb::make_uniq<duckdb::LogicalOrder>(move(orders));
	order_by->AddChild(move(projection));

	auto create_rai =
	    duckdb::make_uniq<duckdb::LogicalCreateRAI>(bound_info.name, *base_get->GetTable(), bound_info.rai_direction,
	                                                base_column_ids, referenced_tables, referenced_column_ids);
	create_rai->AddChild(move(order_by));

	return move(create_rai);
}

static duckdb::unique_ptr<duckdb::LogicalCreateRAI> CreatePlanForNonPKFPRAI(duckdb::BoundCreateRAIInfo &bound_info) {
	duckdb::vector<column_t> referenced_column_ids;
	duckdb::vector<column_t> base_column_ids;
	duckdb::vector<duckdb::TableCatalogEntry *> referenced_tables;

	auto ref_from_tbl = reinterpret_cast<duckdb::BoundBaseTableRef *>(bound_info.referenced_tables[0].get());
	auto ref_to_tbl = reinterpret_cast<duckdb::BoundBaseTableRef *>(bound_info.referenced_tables[1].get());
	auto base_tbl = reinterpret_cast<duckdb::BoundBaseTableRef *>(bound_info.table.get());
	auto ref_from_get = reinterpret_cast<duckdb::LogicalGet *>(ref_from_tbl->get.get());
	auto ref_to_get = reinterpret_cast<duckdb::LogicalGet *>(ref_to_tbl->get.get());
	auto base_get = reinterpret_cast<duckdb::LogicalGet *>(base_tbl->get.get());
	ref_from_get->column_ids.push_back((column_t)-1);
	ref_to_get->column_ids.push_back((column_t)-1);
	base_get->column_ids.push_back((column_t)-1);

	std::string col_name = bound_info.references[0]->GetName();
	const duckdb::ColumnList &entry_from_get = ref_from_get->GetTable()->GetColumns();
	if (entry_from_get.ColumnExists(col_name)) {
		referenced_column_ids.push_back(entry_from_get.GetColumnIndex(col_name).index);
	} else {
		throw duckdb::Exception("Column " + col_name + " in rai not found");
	}
	col_name = bound_info.references[1]->GetName();
	const duckdb::ColumnList &entry_to_get = ref_to_get->GetTable()->GetColumns();
	if (entry_to_get.ColumnExists(col_name)) {
		referenced_column_ids.push_back(entry_to_get.GetColumnIndex(col_name).index);
	} else {
		throw duckdb::Exception("Column " + col_name + " in rai not found");
	}
	for (auto &col : bound_info.columns) {
		std::string col_name = col->GetName();
		const duckdb::ColumnList &entry = base_get->GetTable()->GetColumns();
		if (entry.ColumnExists(col_name)) {
			base_column_ids.push_back(entry.GetColumnIndex(col_name).index);
		} else {
			throw duckdb::Exception("Column " + col->GetName() + " in rai not found");
		}
	}

	referenced_tables.push_back(ref_from_get->GetTable().get());
	referenced_tables.push_back(ref_to_get->GetTable().get());

	duckdb::LogicalGet *get_ref_from_tbl = (duckdb::LogicalGet *)ref_from_tbl->get.get();
	duckdb::LogicalGet *get_base_tbl = (duckdb::LogicalGet *)base_tbl->get.get();
	duckdb::LogicalGet *get_ref_to_tbl = (duckdb::LogicalGet *)ref_to_tbl->get.get();

	int ref_from_tbl_index = get_ref_from_tbl->table_index;
	int base_tbl_index = get_base_tbl->table_index;
	int ref_to_tbl_index = get_ref_to_tbl->table_index;

	auto join = duckdb::make_uniq<duckdb::LogicalComparisonJoin>(duckdb::JoinType::LEFT);
	join->AddChild(move(base_tbl->get));
	join->AddChild(move(ref_from_tbl->get));
	duckdb::JoinCondition join_condition;
	join_condition.comparison = duckdb::ExpressionType::COMPARE_EQUAL;
	join_condition.left = move(bound_info.columns[0]);
	join_condition.right = move(bound_info.references[0]);
	join->conditions.push_back(move(join_condition));

	auto parent_join = duckdb::make_uniq<duckdb::LogicalComparisonJoin>(duckdb::JoinType::LEFT);
	parent_join->AddChild(move(join));
	parent_join->AddChild(move(ref_to_tbl->get));
	duckdb::JoinCondition parent_join_condition;
	parent_join_condition.comparison = duckdb::ExpressionType::COMPARE_EQUAL;
	parent_join_condition.left = move(bound_info.columns[1]);
	parent_join_condition.right = move(bound_info.references[1]);
	parent_join->conditions.push_back(move(parent_join_condition));

	duckdb::ColumnBinding proj_0(ref_from_tbl_index, 1);
	duckdb::ColumnBinding proj_1(base_tbl_index, 2);
	duckdb::ColumnBinding proj_2(ref_to_tbl_index, 1);
	duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> selection_list;
	selection_list.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(duckdb::LogicalType::BIGINT, proj_0));
	selection_list.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(duckdb::LogicalType::BIGINT, proj_1));
	selection_list.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(duckdb::LogicalType::BIGINT, proj_2));

	auto projection = duckdb::make_uniq<duckdb::LogicalProjection>(4, move(selection_list));
	projection->AddChild(move(parent_join));

	duckdb::ColumnBinding order_0(4, 0);
	duckdb::ColumnBinding order_1(4, 1);
	duckdb::BoundOrderByNode order_0_node(
	    duckdb::OrderType::ASCENDING, duckdb::OrderByNullType::ORDER_DEFAULT,
	    duckdb::make_uniq<duckdb::BoundColumnRefExpression>(duckdb::LogicalType::BIGINT, order_0));
	duckdb::BoundOrderByNode order_1_node(
	    duckdb::OrderType::ASCENDING, duckdb::OrderByNullType::ORDER_DEFAULT,
	    duckdb::make_uniq<duckdb::BoundColumnRefExpression>(duckdb::LogicalType::BIGINT, order_1));
	duckdb::vector<duckdb::BoundOrderByNode> orders;
	orders.push_back(move(order_1_node));
	auto order_by = duckdb::make_uniq<duckdb::LogicalOrder>(move(orders));
	order_by->AddChild(move(projection));

	auto create_rai =
	    duckdb::make_uniq<duckdb::LogicalCreateRAI>(bound_info.name, *base_get->GetTable(), bound_info.rai_direction,
	                                                base_column_ids, referenced_tables, referenced_column_ids);
	create_rai->AddChild(move(order_by));

	return move(create_rai);
}

duckdb::unique_ptr<duckdb::LogicalCreateRAI>
GraphIndexBuilder::CreateLogicalPlan(duckdb::BoundCreateRAIInfo &bound_info) {
	if (bound_info.rai_direction == relgo::GraphIndexDirection::PKFK) {
		return CreatePlanForPKFKRAI(bound_info);
	} else {
		return CreatePlanForNonPKFPRAI(bound_info);
	}
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
GraphIndexBuilder::generateGraphIndexPlan(GraphIndexBuildInfo &build_info) {
	duckdb::unique_ptr<duckdb::CreateStatement> statement = transformCreateRAI(build_info);

	auto binder = duckdb::Binder::CreateBinder(context);
	auto bound_info = BindCreateRAIInfo(*binder, move(statement->info));

	duckdb::PhysicalPlanGeneratorRAI physical_planner(context);
	auto physical_plan = physical_planner.CreatePlan(*bound_info->plan);

	return move(physical_plan);
}

} // namespace relgo