#include "aggregate_parser.hpp"

#include "../operators/physical_aggregate.hpp"
#include "algebra_parser.hpp"
#include "expr_parser.hpp"

namespace relgo {

AggregateType AggregateParser::convertPhysicalAggTypeToAggType(
    const physical::GroupBy_AggFunc_Aggregate type) {
  switch (type) {
  case physical::GroupBy_AggFunc_Aggregate_MAX:
    return AggregateType::MAX_TYPE;
  case physical::GroupBy_AggFunc_Aggregate_MIN:
    return AggregateType::MIN_TYPE;
  case physical::GroupBy_AggFunc_Aggregate_COUNT:
    return AggregateType::COUNT_TYPE;
  case physical::GroupBy_AggFunc_Aggregate_COUNT_DISTINCT:
    return AggregateType::COUNT_DISTINCT_TYPE;
  case physical::GroupBy_AggFunc_Aggregate_SUM:
    return AggregateType::SUM_TYPE;
  default:
    return AggregateType::NULL_TYPE;
  }
  return AggregateType::NULL_TYPE;
}

std::unique_ptr<PhysicalAggregate>
AggregateParser::parseAggregate(const physical::GroupBy &op,
                                std::unique_ptr<PhysicalOperator> child_op,
                                ParseInfo &parse_info) {

  std::unique_ptr<PhysicalAggregate> aggregate_op =
      std::unique_ptr<PhysicalAggregate>(new PhysicalAggregate());

  for (size_t i = 0; i < op.functions_size(); ++i) {
    const physical::GroupBy_AggFunc function = op.functions(i);
    const physical::GroupBy_AggFunc_Aggregate type = function.aggregate();
    AggregateType aggregate_type = convertPhysicalAggTypeToAggType(type);

    bool finish_this_func = false;
    for (size_t var_idx = 0; var_idx < function.vars_size(); ++var_idx) {
      std::vector<std::unique_ptr<VariableExprOpr>> var_opr_list =
          ExprParser::parseVariables(function.vars(var_idx), parse_info);
      for (auto &var_opr : var_opr_list) {
        aggregate_op->groupby.push_back(std::move(var_opr));
        aggregate_op->aggregate_type_list.push_back(aggregate_type);

        if (aggregate_type == AggregateType::COUNT_TYPE &&
            !function.vars(var_idx).has_property()) {
          finish_this_func = true;
          break;
        }
      }

      if (finish_this_func)
        break;
    }
  }

  std::vector<std::string> col_names;
  std::vector<int> col_alias;
  for (int i = 0; i < aggregate_op->groupby.size(); ++i) {
    col_names.push_back(aggregate_op->groupby[i]->variable_name);
    col_alias.push_back(aggregate_op->groupby[i]->table_alias);
  }

  parse_info.project(col_names, col_alias);

  aggregate_op->child = move(child_op);
  return move(aggregate_op);
}

} // namespace relgo