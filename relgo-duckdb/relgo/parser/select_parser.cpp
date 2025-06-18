#include "select_parser.hpp"
#include "algebra_parser.hpp"

namespace relgo {
std::unique_ptr<PhysicalSelect>
SelectParser::parseSelect(const algebra::Select &select,
                          std::unique_ptr<PhysicalOperator> child_op,
                          ParseInfo &parse_info) {
  std::unique_ptr<PhysicalSelect> select_op =
      std::unique_ptr<PhysicalSelect>(new PhysicalSelect());

  std::shared_ptr<ConditionNode> filter_condition =
      AlgebraParser::parsePredicate(select.predicate(), parse_info);
  select_op->filter_conditions = std::move(filter_condition);
  select_op->child = std::move(child_op);

  auto &properties = parse_info.current_table_schema.properties;
  for (size_t i = 0; i < properties.size(); ++i) {
    select_op->types.push_back(properties[i].type);
  }

  return move(select_op);
}
} // namespace relgo