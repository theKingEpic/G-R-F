#ifndef PHYSICAL_SELECT_H
#define PHYSICAL_SELECT_H

#include "condition_tree.hpp"
#include "physical_operator.hpp"

#include <sstream>
#include <string>

namespace relgo {

class PhysicalSelect : public PhysicalOperator {
public:
  PhysicalSelect()
      : PhysicalOperator(PhysicalOperatorType::PHYSICAL_SELECT_TYPE),
        filter_conditions(nullptr), types(std::vector<PhysicalType>()) {}
  ~PhysicalSelect() {}

  std::string toString();

public:
  std::vector<PhysicalType> types;
  std::unique_ptr<PhysicalOperator> child;
  std::shared_ptr<ConditionNode> filter_conditions;
};

} // namespace relgo

#endif