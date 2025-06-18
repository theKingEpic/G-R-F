#include "physical_project.hpp"

namespace relgo {

std::string PhysicalProject::toString() {
  std::ostringstream oss;
  oss << "[Project Operator]"
      << "\n";

  for (int i = 0; i < projection.size(); ++i) {
    oss << projection[i]->toString() << "\n";
  }

  oss << "\n";
  oss << child->toString() << "\n";

  return oss.str();
}

} // namespace relgo