#include "physical_join.hpp"

namespace relgo {

std::string PhysicalJoin::toString() {
  std::ostringstream oss;
  oss << "[Join Operator]" << physicalOperatorTypeToStr(op_type) << "\n";
  oss << "Conditions: "
      << "\n";

  for (int i = 0; i < this->conditions.size(); ++i) {
    if (conditions[i])
      oss << conditions[i]->toString() << "\n";
  }

  oss << left->toString() << "\n";
  oss << right->toString() << "\n";

  return oss.str();
}

} // namespace relgo