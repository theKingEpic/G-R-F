#include "physical_plan_generator_rai.hpp"

#include "duckdb/execution/column_binding_resolver.hpp"
#include "physical_create_rai.hpp"

namespace duckdb {

class DependencyExtractor : public LogicalOperatorVisitor {
public:
	explicit DependencyExtractor(DependencyList &dependencies) : dependencies(dependencies) {
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundFunctionExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		// extract dependencies from the bound function expression
		if (expr.function.dependency) {
			expr.function.dependency(expr, dependencies);
		}
		return nullptr;
	}

private:
	DependencyList &dependencies;
};

PhysicalPlanGeneratorRAI::PhysicalPlanGeneratorRAI(ClientContext &context) : PhysicalPlanGenerator(context) {
}

PhysicalPlanGeneratorRAI::~PhysicalPlanGeneratorRAI() {
}

duckdb::unique_ptr<PhysicalOperator> PhysicalPlanGeneratorRAI::CreatePlan(LogicalCreateRAI &op) {
	ColumnBindingResolver resolver;
	resolver.VisitOperator(op);
	op.ResolveOperatorTypes();

	DependencyExtractor extractor(dependencies);
	extractor.VisitOperator(op);

	assert(op.children.size() == 1);
	dependencies.AddDependency(op.table);

	auto plan = PhysicalPlanGenerator::CreatePlan(*op.children[0]);
	auto create = make_uniq<PhysicalCreateRAI>(op, op.name, op.table, op.rai_direction, op.column_ids,
	                                           op.referenced_tables, op.referenced_columns);
	create->children.push_back(move(plan));
	return move(create);
}

} // namespace duckdb