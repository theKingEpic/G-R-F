#include "../includes/physical_indexed_table_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <utility>

namespace duckdb {

PhysicalIndexedTableScan::PhysicalIndexedTableScan(vector<LogicalType> types, IndexedTableFunction function_p,
                                                   idx_t table_index, unique_ptr<FunctionData> bind_data_p,
                                                   vector<LogicalType> returned_types_p, vector<column_t> column_ids_p,
                                                   vector<unique_ptr<Expression>> filter,
                                                   vector<idx_t> projection_ids_p, vector<string> names_p,
                                                   unique_ptr<TableFilterSet> table_filters_p,
                                                   idx_t estimated_cardinality, ExtraOperatorInfo extra_info)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality),
      function(std::move(function_p)), table_index(table_index), bind_data(std::move(bind_data_p)),
      returned_types(std::move(returned_types_p)), column_ids(std::move(column_ids_p)),
      projection_ids(std::move(projection_ids_p)), names(std::move(names_p)), table_filters(std::move(table_filters_p)),
      extra_info(extra_info), rows_filter(nullptr), rows_count(-1) {
	if (filter.size() > 1) {
		//! create a big AND out of th e expressions
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : filter) {
			conjunction->children.push_back(move(expr));
		}
		expression = move(conjunction);
	} else if (filter.size() == 1) {
		expression = move(filter[0]);
	}

	auto &bind_data_ptr = bind_data->Cast<TableScanBindData>();
	auto &storage = bind_data_ptr.table.GetStorage();

	TableScanState state;
	vector<column_t> column_ids(1, 0);
	storage.InitializeScan(state, column_ids);
	indexed_collection = make_shared<IndexedRowGroupCollection>(state.table_state.row_group->GetCollection());
}

class TableScanGlobalSourceState : public GlobalSourceState {
public:
	TableScanGlobalSourceState(ClientContext &context, const PhysicalIndexedTableScan &op) {
		if (op.function.init_global) {
			IndexedTableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids,
			                                    op.table_filters.get());
			input.input_rai.indexed_collection = op.indexed_collection;

			auto &bind_data = op.bind_data->Cast<TableScanBindData>();
			auto &storage = bind_data.table.GetStorage();
			auto &local_storage = LocalStorage::Get(context, storage.db);
			TableScanState table_scan_state;
			CollectionScanState scan_state(table_scan_state);
			local_storage.InitializeScan(storage, scan_state, nullptr);

			if (scan_state.row_group) {
				input.input_rai.indexed_local_collection =
				    make_shared<IndexedRowGroupCollection>(scan_state.row_group->GetCollection());
			}

			global_state = op.function.init_global(context, input);
			if (global_state) {
				max_threads = global_state->MaxThreads();
			}
		} else {
			max_threads = 1;
		}
	}

	idx_t max_threads = 0;
	unique_ptr<GlobalTableFunctionState> global_state;

	idx_t MaxThreads() override {
		return max_threads;
	}
};

class TableScanLocalSourceState : public LocalSourceState {
public:
	TableScanLocalSourceState(ExecutionContext &context, TableScanGlobalSourceState &gstate,
	                          const PhysicalIndexedTableScan &op) {
		if (op.function.init_local) {
			IndexedTableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids,
			                                    op.table_filters.get());
			input.input_rai =
			    IndexedTableFunctionInitInputRAI(nullptr, nullptr, op.rows_filter, op.row_bitmask, op.zone_bitmask,
			                                     op.rows_count, op.table_filters.get(), nullptr, nullptr);
			input.input_rai.indexed_collection = op.indexed_collection;

			auto &bind_data = op.bind_data->Cast<TableScanBindData>();
			auto &storage = bind_data.table.GetStorage();
			auto &local_storage = LocalStorage::Get(context.client, storage.db);

			TableScanState table_scan_state;
			CollectionScanState scan_state(table_scan_state);
			local_storage.InitializeScan(storage, scan_state, nullptr);

			if (scan_state.row_group) {
				input.input_rai.indexed_local_collection =
				    make_shared<IndexedRowGroupCollection>(scan_state.row_group->GetCollection());
			}

			local_state = op.function.init_local(context, input, gstate.global_state.get());
		}

		// if (op.expression != nullptr) {
		//    executor = make_shared<ExpressionExecutor>(context.client,
		//    op.expression.get());
		// }
	}

	unique_ptr<LocalTableFunctionState> local_state;
};

unique_ptr<LocalSourceState> PhysicalIndexedTableScan::GetLocalSourceState(ExecutionContext &context,
                                                                           GlobalSourceState &gstate) const {
	return make_uniq<TableScanLocalSourceState>(context, gstate.Cast<TableScanGlobalSourceState>(), *this);
}

unique_ptr<GlobalSourceState> PhysicalIndexedTableScan::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<TableScanGlobalSourceState>(context, *this);
}

SourceResultType PhysicalIndexedTableScan::GetData(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	D_ASSERT(!column_ids.empty());
	auto &gstate = input.global_state.Cast<TableScanGlobalSourceState>();
	auto &state = input.local_state.Cast<TableScanLocalSourceState>();

	shared_ptr<IndexedRowGroupCollection> indexed_local_collection = nullptr;

	auto &bind_data_p = this->bind_data->Cast<TableScanBindData>();
	auto &storage = bind_data_p.table.GetStorage();
	auto &local_storage = LocalStorage::Get(context.client, storage.db);

	TableScanState table_scan_state;
	CollectionScanState scan_state(table_scan_state);
	local_storage.InitializeScan(storage, scan_state, nullptr);

	if (scan_state.row_group) {
		indexed_local_collection = make_shared<IndexedRowGroupCollection>(scan_state.row_group->GetCollection());
	}

	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	function.function(context.client, data, chunk, indexed_collection.get(), indexed_local_collection.get());

	// if (true) {
	// for (int i = 0; i < chunk.data.size(); ++i) {
	//    std::cout << chunk.data[i].ToString() << std::endl;
	//}
	// std::cout << "table scan output: " << " " << chunk.size() << std::endl <<
	// std::endl;
	// }
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

double PhysicalIndexedTableScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	if (function.table_scan_progress) {
		return function.table_scan_progress(context, bind_data.get(), gstate.global_state.get());
	}
	// if table_scan_progress is not implemented we don't support this function
	// yet in the progress bar
	return -1;
}

idx_t PhysicalIndexedTableScan::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                              LocalSourceState &lstate) const {
	D_ASSERT(SupportsBatchIndex());
	D_ASSERT(function.get_batch_index);
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	return function.get_batch_index(context.client, bind_data.get(), state.local_state.get(),
	                                gstate.global_state.get());
}

string PhysicalIndexedTableScan::GetName() const {
	return StringUtil::Upper(function.name + " " + function.extra_info);
}

string PhysicalIndexedTableScan::ParamsToString() const {
	string result;
	if (function.to_string) {
		result = function.to_string(bind_data.get());
		result += "\n[INFOSEPARATOR]\n";
	}
	if (function.projection_pushdown) {
		if (function.filter_prune) {
			for (idx_t i = 0; i < projection_ids.size(); i++) {
				const auto &column_id = column_ids[projection_ids[i]];
				if (column_id < names.size()) {
					if (i > 0) {
						result += "\n";
					}
					result += names[column_id];
				}
			}
		} else {
			for (idx_t i = 0; i < column_ids.size(); i++) {
				const auto &column_id = column_ids[i];
				if (column_id < names.size()) {
					if (i > 0) {
						result += "\n";
					}
					result += names[column_id];
				}
			}
		}
	}
	if (function.filter_pushdown && table_filters) {
		result += "\n[INFOSEPARATOR]\n";
		result += "Filters: ";
		for (auto &f : table_filters->filters) {
			auto &column_index = f.first;
			auto &filter = f.second;
			if (column_index < names.size()) {
				result += filter->ToString(names[column_ids[column_index]]);
				result += "\n";
			}
		}
	}
	if (!extra_info.file_filters.empty()) {
		result += "\n[INFOSEPARATOR]\n";
		result += "File Filters: " + extra_info.file_filters;
	}
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("EC: %llu", estimated_cardinality);
	return result;
}

bool PhysicalIndexedTableScan::Equals(const PhysicalOperator &other_p) const {
	if (type != other_p.type) {
		return false;
	}
	auto &other = other_p.Cast<PhysicalIndexedTableScan>();
	if (function.function != other.function.function) {
		return false;
	}
	if (column_ids != other.column_ids) {
		return false;
	}
	if (!FunctionData::Equals(bind_data.get(), other.bind_data.get())) {
		return false;
	}
	return true;
}

} // namespace duckdb
