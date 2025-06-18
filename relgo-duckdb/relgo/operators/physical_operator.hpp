#ifndef PHYSICAL_OPERATOR_H
#define PHYSICAL_OPERATOR_H

#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "../proto_generated/physical.pb.h"
#include "../utils/types.hpp"

namespace relgo {

enum class PhysicalOperatorType {
  PHYSICAL_OP_TYPE,
  PHYSICAL_SCAN_TYPE,
  PHYSICAL_JOIN_TYPE,
  PHYSICAL_HASH_JOIN_TYPE,
  PHYSICAL_ADJ_JOIN_TYPE,
  PHYSICAL_NEIGHBOR_JOIN_TYPE,
  PHYSICAL_EXTEND_INTERSECT_JOIN_TYPE,
  PHYSICAL_PROJECT_TYPE,
  PHYSICAL_AGGREGATE_TYPE,
  PHYSICAL_SELECT_TYPE
};

inline std::string physicalOperatorTypeToStr(PhysicalOperatorType &type) {
  switch (type) {
  case PhysicalOperatorType::PHYSICAL_OP_TYPE:
    return "PHYSICAL_OP_TYPE";
  case PhysicalOperatorType::PHYSICAL_SCAN_TYPE:
    return "PHYSICAL_SCAN_TYPE";
  case PhysicalOperatorType::PHYSICAL_JOIN_TYPE:
    return "PHYSICAL_JOIN_TYPE";
  case PhysicalOperatorType::PHYSICAL_HASH_JOIN_TYPE:
    return "PHYSICAL_HASH_JOIN_TYPE";
  case PhysicalOperatorType::PHYSICAL_ADJ_JOIN_TYPE:
    return "PHYSICAL_ADJ_JOIN_TYPE";
  case PhysicalOperatorType::PHYSICAL_NEIGHBOR_JOIN_TYPE:
    return "PHYSICAL_NEIGHBOR_JOIN_TYPE";
  case PhysicalOperatorType::PHYSICAL_PROJECT_TYPE:
    return "PHYSICAL_PROJECT_TYPE";
  case PhysicalOperatorType::PHYSICAL_EXTEND_INTERSECT_JOIN_TYPE:
    return "PHYSICAL_EXTEND_INTERSECT_JOIN_TYPE";
  case PhysicalOperatorType::PHYSICAL_AGGREGATE_TYPE:
    return "PHYSICAL_AGGREGATE_TYPE";
  case PhysicalOperatorType::PHYSICAL_SELECT_TYPE:
    return "PHYSICAL_SELECT_TYPE";
  default:
    return "UNKNOWN TYPE";
  }
}

class PhysicalOperator {
public:
  PhysicalOperator(
      PhysicalOperatorType op = PhysicalOperatorType::PHYSICAL_OP_TYPE)
      : op_type(op) {}
  virtual ~PhysicalOperator() = default;

  virtual std::string toString() {
    std::cerr << "Invoke virtual function toString of PhysicalOperator."
              << std::endl;
    return "";
  }
  virtual void fromPb(physical::PhysicalPlan &pb_plan) {
    std::cerr << "Invoke virtual function fromPb of PhysicalOperator."
              << std::endl;
  }

public:
  PhysicalOperatorType op_type;
  std::vector<std::string> prop_list;
  std::vector<PhysicalType> prop_type_list;
  std::unordered_map<std::string, int> prop_index_map;
};

} // namespace relgo

#endif