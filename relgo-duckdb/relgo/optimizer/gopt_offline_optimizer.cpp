#include "gopt_offline_optimizer.hpp"
#include "../planner/graph_planner.hpp"
#include <chrono>

namespace relgo {

void GOptOfflineOptimizer::setOptimizerConfigPath(
    std::string _optimize_config_path) {
  this->optimize_config_path = _optimize_config_path;
}

bool GOptOfflineOptimizer::checkOptimizerConfigInfo() {
  if (this->optimize_config_path.empty() ||
      !check_path_exits(this->optimize_config_path))
    return false;
  return true;
}

physical::PhysicalPlan GOptOfflineOptimizer::optimize(std::string query) {
  if (!this->checkOptimizerConfigInfo()) {
    std::cerr << "config path is set by mistake" << std::endl;
  }

  std::cout << "the query is " << query << std::endl;

  if (hasSchemaContent() && hasStatisticsContent()) {
    auto plan = graph_planner_wrapper->CompilePlan(
        optimize_config_path, query, schema_content, statistics_content);

    return plan.physical_plan;
  } else {
    std::cerr << "the content of schema or statistics is not set" << std::endl;
  }
}

} // namespace relgo