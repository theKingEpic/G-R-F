#include "condition_tree.hpp"

namespace relgo {
// ConditionNode 实现
ConditionConstNode::ConditionConstNode(const std::string &_field,
                                       PhysicalType _field_type,
                                       int _field_index, std::string &_table_id,
                                       std::string &_table_name,
                                       int _table_alias)
    : ConditionNode(ConditionNodeType::CONST_VARIABLE_NODE), field(_field),
      field_type(_field_type), field_index(_field_index), table_id(_table_id),
      table_name(_table_name), table_alias(_table_alias) {}

ConditionConstNode::ConditionConstNode(Value _value)
    : ConditionNode(ConditionNodeType::CONST_VALUE_NODE), value(_value) {}

std::string ConditionConstNode::toString(int indent) const {
  std::ostringstream oss;
  std::string indentation(indent, ' ');
  if (node_type == ConditionNodeType::CONST_VALUE_NODE) {
    oss << indentation << "Const Value: " << value.ToString() << "\n";
  } else {
    oss << indentation << "Const Variable: " << table_alias << "(" << table_id
        << ")"
        << "." << field << ":" << physicalTypeToString(field_type) << "\n";
  }
  return oss.str();
}

// OperatorNode 实现
ConditionOperatorNode::ConditionOperatorNode(TripletCompType opType)
    : ConditionNode(ConditionNodeType::OPERATOR_NODE), opType(opType) {}

void ConditionOperatorNode::addOperand(std::shared_ptr<ConditionNode> operand) {
  operands.push_back(operand);
}

std::string ConditionOperatorNode::toString(int indent) const {
  std::ostringstream oss;
  std::string indentation(indent, ' ');
  oss << indentation << fromTripletCompTypeToStr(opType) << "\n";
  for (const auto &operand : operands) {
    oss << operand->toString(indent + 2);
  }

  return oss.str();
}

} // namespace relgo
