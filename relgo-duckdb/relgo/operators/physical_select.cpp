#include "physical_select.hpp"

namespace relgo {

std::string PhysicalSelect::toString() {
  std::ostringstream oss;
  oss << "[Filter Operator]"
      << "\n";

  for (const auto &type : types) {
    oss << physicalTypeToString(type) << ",";
  }
  oss << "\n";

  if (filter_conditions) {
    oss << "Filter Conditions:\n";
    oss << filter_conditions->toString() << "\n";
  }

  oss << "\n";
  oss << child->toString() << "\n";

  return oss.str();
}

} // namespace relgo