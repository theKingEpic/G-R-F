#ifndef PHYSICAL_PROJECT_HPP
#define PHYSICAL_PROJECT_HPP

#include "../parser/expr_parser.hpp"
#include "condition_tree.hpp"
#include "physical_operator.hpp"
#include <sstream>
#include <string>
#include <vector>

namespace relgo {

class PhysicalProject : public PhysicalOperator {
public:
  PhysicalProject()
      : PhysicalOperator(PhysicalOperatorType::PHYSICAL_PROJECT_TYPE) {}
  PhysicalProject(std::vector<std::shared_ptr<ConditionNode>> &_projections)
      : PhysicalOperator(PhysicalOperatorType::PHYSICAL_PROJECT_TYPE),
        projection(move(_projections)) {}
  ~PhysicalProject() {}

  std::string toString();

public:
  std::unique_ptr<PhysicalOperator> child;
  std::vector<std::shared_ptr<ConditionNode>> projection;
};

} // namespace relgo

#endif