#include "physical_create_rai.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/table_io_manager.hpp"

using namespace duckdb;

class CreateRAIState : public CachingOperatorState {
public:
	explicit CreateRAIState(ExecutionContext &context, const PhysicalCreateRAI &op) {
		vector<LogicalType> types;
		if (op.rai_direction == relgo::GraphIndexDirection::SELF ||
		    op.rai_direction == relgo::GraphIndexDirection::UNDIRECTED) {
			for (int i = 0; i < 3; ++i) {
				types.push_back(LogicalType::BIGINT);
			}
		} else if (op.rai_direction == relgo::GraphIndexDirection::PKFK) {
			for (int i = 0; i < 2; ++i) {
				types.push_back(LogicalType::BIGINT);
			}
		} else {
			std::cout << "unsolved rai_drection in create rai state" << std::endl;
		}
		collection.Initialize(context.client, types);
	}

	DataChunk collection;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
	}
};

class RAIGlobalSinkState : public GlobalSinkState {
public:
	RAIGlobalSinkState(ClientContext &context_p, const PhysicalCreateRAI &op) : context(context_p) {
		vector<LogicalType> types;
		if (op.rai_direction == relgo::GraphIndexDirection::SELF ||
		    op.rai_direction == relgo::GraphIndexDirection::UNDIRECTED) {
			for (int i = 0; i < 3; ++i) {
				types.push_back(LogicalType::BIGINT);
			}
		} else if (op.rai_direction == relgo::GraphIndexDirection::PKFK) {
			for (int i = 0; i < 2; ++i) {
				types.push_back(LogicalType::BIGINT);
			}
		} else {
			std::cout << "unsolved rai_drection in create rai state" << std::endl;
		}
		collection.Initialize(context, types);
	}

public:
	ClientContext &context;
	DataChunk collection;
	mutex lock;
};

class RAILocalSinkState : public LocalSinkState {
public:
	RAILocalSinkState(ClientContext &context, const PhysicalCreateRAI &op) {
		vector<LogicalType> types;
		if (op.rai_direction == relgo::GraphIndexDirection::SELF ||
		    op.rai_direction == relgo::GraphIndexDirection::UNDIRECTED) {
			for (int i = 0; i < 3; ++i) {
				types.push_back(LogicalType::BIGINT);
			}
		} else if (op.rai_direction == relgo::GraphIndexDirection::PKFK) {
			for (int i = 0; i < 2; ++i) {
				types.push_back(LogicalType::BIGINT);
			}
		} else {
			std::cout << "unsolved rai_drection in create rai state" << std::endl;
		}
		collection.Initialize(context, types);
	}

public:
	DataChunk collection;
};

OperatorResultType PhysicalCreateRAI::ExecuteInternal(ExecutionContext &context, DataChunk &inputs, DataChunk &chunk,
                                                      GlobalOperatorState &gstate, OperatorState &state_p) const {

	auto &state = state_p.Cast<CreateRAIState>();
	auto &sink = sink_state->Cast<RAIGlobalSinkState>();

	assert(children.size() == 1);
	std::vector<std::string> referenced_table_names;
	for (int i = 0; i < referenced_tables.size(); ++i) {
		referenced_table_names.push_back(referenced_tables[i]->name);
	}

	unique_ptr<relgo::GraphIndex<int64_t *, SelectionVector>> rai =
	    make_uniq<relgo::GraphIndex<int64_t *, SelectionVector>>(name, table.name, rai_direction, column_ids,
	                                                              referenced_table_names, referenced_columns);
	rai->alist->source_num = referenced_tables[0]->GetStorage().info->cardinality;
	rai->alist->target_num = referenced_tables[1]->GetStorage().info->cardinality;
	idx_t count = 0;
	idx_t chunk_count = sink.collection.size();

	if (chunk_count != 0) {
#if ENABLE_ALISTS
		if (rai->rai_direction == relgo::GraphIndexDirection::PKFK) {
			UnifiedVectorFormat sink_data0, sink_data1;
			sink.collection.data[0].ToUnifiedFormat(chunk_count, sink_data0);
			sink.collection.data[1].ToUnifiedFormat(chunk_count, sink_data1);
			auto rdata0 = (int64_t *)sink_data0.data;
			auto rdata1 = (int64_t *)sink_data1.data;

			std::vector<relgo::EdgeTuple> tuples;
			for (int i = 0; i < chunk_count; ++i) {
				tuples.push_back(relgo::EdgeTuple(rdata0[i], rdata1[i], rdata0[i]));
			}

			rai->alist->AppendPKFK(tuples, chunk_count);
		} else {
			UnifiedVectorFormat sink_data0, sink_data1, sink_data2;
			sink.collection.data[0].ToUnifiedFormat(chunk_count, sink_data0);
			sink.collection.data[1].ToUnifiedFormat(chunk_count, sink_data1);
			sink.collection.data[2].ToUnifiedFormat(chunk_count, sink_data2);
			auto rdata0 = (int64_t *)sink_data0.data;
			auto rdata1 = (int64_t *)sink_data1.data;
			auto rdata2 = (int64_t *)sink_data2.data;

			std::vector<relgo::EdgeTuple> tuples;
			for (int i = 0; i < chunk_count; ++i) {
				tuples.push_back(relgo::EdgeTuple(rdata0[i], rdata1[i], rdata2[i]));
			}

			rai->alist->Append(tuples, chunk_count, rai_direction);
		}
#endif
		count += chunk_count;
	}

#if ENABLE_ALISTS
	rai->alist->Finalize(rai_direction);
#endif

	// table.GetStorage().AddRAI(move(rai));
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(count));

	return OperatorResultType::NEED_MORE_INPUT;
}

void PhysicalCreateRAI::EnlargeTable(ClientContext &context, DataChunk &input) const {
	// auto &state = state_p.Cast<CreateRAIState>();
	auto &sink = sink_state->Cast<RAIGlobalSinkState>();

	assert(children.size() == 1);
	vector<std::string> referenced_table_names;
	for (int i = 0; i < referenced_tables.size(); ++i) {
		referenced_table_names.push_back(referenced_tables[i]->name);
	}

	auto &storage = table.GetStorage();
	unique_ptr<duckdb::PhysicalGraphIndex> graph_index = make_uniq<duckdb::PhysicalGraphIndex>(
	    column_ids, TableIOManager::Get(storage), vector<unique_ptr<duckdb::Expression>>(), IndexConstraintType::NONE,
	    storage.db, name, table.name, rai_direction, referenced_table_names, referenced_columns);

	auto &rai = graph_index->graph_index;
	rai->alist->source_num = referenced_tables[0]->GetStorage().info->cardinality;
	rai->alist->target_num = referenced_tables[1]->GetStorage().info->cardinality;
	idx_t count = 0;

	idx_t chunk_count = input.size();

	if (chunk_count != 0) {
#if ENABLE_ALISTS
		if (rai->rai_direction == relgo::GraphIndexDirection::PKFK) {
			UnifiedVectorFormat sink_data0, sink_data1;
			sink.collection.data[0].ToUnifiedFormat(chunk_count, sink_data0);
			sink.collection.data[1].ToUnifiedFormat(chunk_count, sink_data1);
			auto rdata0 = (int64_t *)sink_data0.data;
			auto rdata1 = (int64_t *)sink_data1.data;

			std::vector<relgo::EdgeTuple> tuples;
			for (int i = 0; i < chunk_count; ++i) {
				tuples.push_back(relgo::EdgeTuple(rdata0[i], rdata1[i], rdata0[i]));
			}

			rai->alist->AppendPKFK(tuples, chunk_count);
		} else {
			UnifiedVectorFormat sink_data0, sink_data1, sink_data2;
			sink.collection.data[0].ToUnifiedFormat(chunk_count, sink_data0);
			sink.collection.data[1].ToUnifiedFormat(chunk_count, sink_data1);
			sink.collection.data[2].ToUnifiedFormat(chunk_count, sink_data2);
			auto rdata0 = (int64_t *)sink_data0.data;
			auto rdata1 = (int64_t *)sink_data1.data;
			auto rdata2 = (int64_t *)sink_data2.data;

			std::vector<relgo::EdgeTuple> tuples;
			for (int i = 0; i < chunk_count; ++i) {
				tuples.push_back(relgo::EdgeTuple(rdata0[i], rdata1[i], rdata2[i]));
			}

			rai->alist->Append(tuples, chunk_count, rai_direction);
		}
#endif
		count += chunk_count;
	}

#if ENABLE_ALISTS
	rai->alist->Finalize(rai_direction);
#endif

	storage.info->indexes.AddIndex(std::move(graph_index));
	// table.GetStorage().AddRAI(move(rai));
}

unique_ptr<OperatorState> PhysicalCreateRAI::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<CreateRAIState>(context, *this);
}

SinkResultType PhysicalCreateRAI::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<RAILocalSinkState>();
	auto &gstate = input.global_state.Cast<RAIGlobalSinkState>();

	gstate.collection.Append(chunk, true);
	// lstate.collection.Append(chunk, true);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCreateRAI::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	// auto &gstate = input.global_state.Cast<RAIGlobalSinkState>();
	// auto &lstate = input.local_state.Cast<RAILocalSinkState>();

	// lock_guard<mutex> local_rai_lock(gstate.lock);
	// gstate.collection.Append(lstate.collection, true);

	// EnlargeTable(context, lstate.collection, gstate.collection);

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCreateRAI::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
	auto &sink = input.global_state.Cast<RAIGlobalSinkState>();
	EnlargeTable(context, sink.collection);
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalCreateRAI::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RAIGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalCreateRAI::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<RAILocalSinkState>(context.client, *this);
}

SourceResultType PhysicalCreateRAI::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}