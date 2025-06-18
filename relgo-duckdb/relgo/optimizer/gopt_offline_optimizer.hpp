#ifndef GOPT_OFFLINE_OPTIMIZER_H
#define GOPT_OFFLINE_OPTIMIZER_H

#include "../planner/graph_planner.hpp"
#include "../utils/files.hpp"
#include "graph_offline_optimizer.hpp"

#include <cstdlib>
#include <iostream>
#include <string>

namespace relgo {

class GOptOfflineOptimizer : public GraphOfflineOptimizer {
public:
	GOptOfflineOptimizer() : GraphOfflineOptimizer("", ""), lib_path(""), natvie_path("") {
	}
	GOptOfflineOptimizer(std::string _lib_path, std::string _natvie_path)
	    : GraphOfflineOptimizer("", ""), lib_path(_lib_path), natvie_path(_natvie_path) {
		optimize_config_path = "";

		graph_planner_wrapper =
		    std::unique_ptr<gs::GraphPlannerWrapper>(new gs::GraphPlannerWrapper(_lib_path, _natvie_path));
		// std::make_unique<gs::GraphPlannerWrapper>(_lib_path, _natvie_path);
	}

	physical::PhysicalPlan optimize(std::string query) override;
	void setOptimizerConfigPath(std::string _optimize_config_path);
	void setGraphContent(std::string _schema_content, std::string _statistics_content);
	bool checkOptimizerConfigInfo();

	void setSchemaContent(std::string &_schema_content) {
		schema_content = _schema_content;
	}

	bool hasSchemaContent() {
		return !schema_content.empty();
	}

	void setStatisticsContent(std::string &_statistics_content) {
		statistics_content = _statistics_content;
	}

	bool hasStatisticsContent() {
		return !statistics_content.empty();
	}

	std::string lib_path;
	std::string natvie_path;
	std::string optimize_config_path;

	std::string schema_content;
	std::string statistics_content;

	std::unique_ptr<gs::GraphPlannerWrapper> graph_planner_wrapper;
};

} // namespace relgo

#endif