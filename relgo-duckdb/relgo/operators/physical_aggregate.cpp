#include "physical_aggregate.hpp"

namespace relgo {

std::string PhysicalAggregate::toString() {
  std::ostringstream oss;
  oss << "[Aggregate Operator]"
      << "\n";

  for (int i = 0; i < groupby.size(); ++i) {
    oss << groupby[i]->variable_name << " "
        << convertAggTypeToStr(aggregate_type_list[i]) << "\n";
  }

  oss << "\n";
  oss << child->toString() << "\n";

  return oss.str();
}

} // namespace relgo