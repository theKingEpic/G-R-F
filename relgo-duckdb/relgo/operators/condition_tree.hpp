#ifndef CONDITION_TREE_H
#define CONDITION_TREE_H

#include "../utils/types.hpp"
#include "../utils/value.hpp"
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <variant>
#include <vector>

namespace relgo {

enum class TripletCompType : uint8_t {
  EQ = 0,
  NE = 1,
  LT = 2,
  LE = 3,
  GT = 4,
  GE = 5,
  WITHIN = 6,
  WITHOUT = 7,
  STARTSWITH = 8,
  ENDSWITH = 9,
  AND = 10,
  OR = 11,
  NOT = 12,
  ISNULL = 13,
  REGEX = 14,
  UNKNOWN = 15
};

inline TripletCompType fromStrToTripletCompType(std::string str) {
  if (str == "EQ")
    return TripletCompType::EQ;
  else if (str == "NE")
    return TripletCompType::NE;
  else if (str == "LT")
    return TripletCompType::LT;
  else if (str == "LE")
    return TripletCompType::LE;
  else if (str == "GT")
    return TripletCompType::GT;
  else if (str == "GE")
    return TripletCompType::GE;
  else if (str == "WITHIN")
    return TripletCompType::WITHIN;
  else if (str == "WITHOUT")
    return TripletCompType::WITHOUT;
  else if (str == "STARTSWITH")
    return TripletCompType::STARTSWITH;
  else if (str == "ENDSWITH")
    return TripletCompType::ENDSWITH;
  else if (str == "AND")
    return TripletCompType::AND;
  else if (str == "OR")
    return TripletCompType::OR;
  else if (str == "NOT")
    return TripletCompType::NOT;
  else if (str == "ISNULL")
    return TripletCompType::ISNULL;
  else if (str == "REGEX")
    return TripletCompType::REGEX;
  else {
    std::cout << "Unsupported triplet comp type" << std::endl;
  }
  return TripletCompType::UNKNOWN;
}

inline std::string fromTripletCompTypeToStr(TripletCompType type) {
  switch (type) {
  case TripletCompType::EQ:
    return "EQ";
  case TripletCompType::NE:
    return "NE";
  case TripletCompType::LT:
    return "LT";
  case TripletCompType::LE:
    return "LE";
  case TripletCompType::GT:
    return "GT";
  case TripletCompType::GE:
    return "GE";
  case TripletCompType::WITHIN:
    return "WITHIN";
  case TripletCompType::WITHOUT:
    return "WITHOUT";
  case TripletCompType::STARTSWITH:
    return "STARTSWITH";
  case TripletCompType::ENDSWITH:
    return "ENDSWITH";
  case TripletCompType::AND:
    return "AND";
  case TripletCompType::OR:
    return "OR";
  case TripletCompType::NOT:
    return "NOT";
  case TripletCompType::ISNULL:
    return "ISNULL";
  case TripletCompType::REGEX:
    return "REGEX";
  case TripletCompType::UNKNOWN:
  default:
    return "UNKNOWN";
  }
}

enum class ConditionNodeType {
  NULL_TYPE,
  CONST_VALUE_NODE,
  CONST_VARIABLE_NODE,
  OPERATOR_NODE
};

// 节点基类
class ConditionNode {
public:
  ConditionNode(ConditionNodeType _type = ConditionNodeType::NULL_TYPE)
      : node_type(_type) {}
  virtual ~ConditionNode() {}
  virtual std::string toString(int indent = 0) const = 0;
  ConditionNodeType node_type;
};

// 条件节点
class ConditionConstNode : public ConditionNode {
public:
  ConditionConstNode(const std::string &field, PhysicalType field_type,
                     int field_index, std::string &table_id,
                     std::string &table_name, int table_alias);
  ConditionConstNode(Value value);
  std::string toString(int indent = 0) const override;

public:
  std::string field;
  PhysicalType field_type;
  int field_index;
  std::string table_id;
  std::string table_name;
  int table_alias;
  Value value;
};

// 操作节点
class ConditionOperatorNode : public ConditionNode {
public:
  ConditionOperatorNode(TripletCompType opType);
  void addOperand(std::shared_ptr<ConditionNode> operand);
  std::string toString(int indent = 0) const override;

public:
  TripletCompType opType;
  std::vector<std::shared_ptr<ConditionNode>> operands;
};
} // namespace relgo

#endif