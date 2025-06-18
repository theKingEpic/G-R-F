#ifndef PLAN_IR_H
#define PLAN_IR_H

#include "../graph/graph_schema.hpp"
#include "../index/graph_index_manager.hpp"
#include "../optimizer/gopt_offline_optimizer.hpp"
#include "../parser/core_parser.hpp"
#include "../parser/parse_info.hpp"
#include "../proto_generated/physical.pb.h"
#include "../third_party/nlohmann_json/json.hpp"
#include "../utils/functions.hpp"

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace relgo {

template <typename T, typename S, typename ES, typename VS> class PlanIR {
public:
  PlanIR()
      : resource_folder("../../resource/"
                        "graph-planner-jni/"),
        output_folder("../../resource/tmp/") {}
  virtual ~PlanIR() = default;

  virtual void loadGraphSchema() {
    std::cerr << "loadGraphSchema is not implemented" << std::endl;
  }

  //! generate th graph.yaml and graph_connection.yaml
  virtual void loadForeignKeys() {
    std::cerr << "loadForeignKeys is not implemented" << std::endl;
  }

  virtual void loadGraphIndex() {
    std::cerr << "loadGraphIndex is not implemented" << std::endl;
  }

  virtual S convertType(relgo::PhysicalType) {
    std::cerr << "error" << std::endl;
  }
  virtual ES convertExprType(relgo::TripletCompType) {
    std::cerr << "error" << std::endl;
  }
  virtual VS convertValue(relgo::Value) { std::cerr << "error" << std::endl; }

  virtual T convert(std::unique_ptr<relgo::PhysicalOperator> op) {
    std::cerr << "error" << std::endl;
  }

  virtual relgo::PhysicalType convertTypeRev(S) {
    std::cerr << "error" << std::endl;
  }

  void initOptimizer() {
    std::string gopt_lib_path = resource_folder + "libs";
    // std::string gopt_native_path = resource_folder / "native";
    std::string gopt_native_path = "";
    std::string gopt_config_path =
        resource_folder + "conf/gs_interactive_hiactor.yaml";

    YAML::Node schema_content = graph_schema.generateGraphYAML();
    std::string schema_content_str = yaml2str(schema_content);

    nlohmann::json statistics_content = graph_schema.generateStatistics();
    std::string statistics_content_str = json2str(statistics_content);

    gopt = GOptOfflineOptimizer(gopt_lib_path, gopt_native_path);

    gopt.setOptimizerConfigPath(gopt_config_path);
    gopt.setSchemaContent(schema_content_str);
    gopt.setStatisticsContent(statistics_content_str);
  }

  void init() {
    loadGraphSchema();
    loadForeignKeys();
    loadGraphIndex();

    initOptimizer();
  }

  // void initialize() { formulateGraphFiles(); }

  //! given a sql query, translate to cypher query, optimize it
  //! and return an optimized plan
  T optimize(std::string query) {
    std::chrono::time_point<std::chrono::system_clock,
                            std::chrono::milliseconds>
        start_2 = std::chrono::time_point_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now());

    physical::PhysicalPlan physical_plan = gopt.optimize(query);
    std::cout << "physical plan: " << std::endl;
    std::cout << physical_plan.DebugString() << std::endl;

    std::chrono::time_point<std::chrono::system_clock,
                            std::chrono::milliseconds>
        end_2 = std::chrono::time_point_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now());
    // if (query[0] == 'S' || query[0] == 's') {
    // execution_query.push_back(query);
    std::cout << "OPTIMIZE TIME: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end_2 -
                                                                       start_2)
                     .count()
              << std::endl;

    std::chrono::time_point<std::chrono::system_clock,
                            std::chrono::milliseconds>
        start = std::chrono::time_point_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now());

    GraphSchema gs_back(graph_schema);
    IndexManager index_back(graph_index_manager);
    index_back.initFromPb(physical_plan);

    ParseInfo parse_info(gs_back, index_back);

    CoreParser core_parser;
    std::unique_ptr<PhysicalOperator> result =
        move(core_parser.parse(physical_plan, parse_info));

    std::chrono::time_point<std::chrono::system_clock,
                            std::chrono::milliseconds>
        end = std::chrono::time_point_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now());
    // if (query[0] == 'S' || query[0] == 's') {
    // execution_query.push_back(query);
    std::cout << "PARSE TIME: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                       start)
                     .count()
              << std::endl;
    //}

    std::cout << result->toString() << std::endl;

    start = std::chrono::time_point_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now());

    auto res = convert(move(result));

    end = std::chrono::time_point_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now());

    // if (query[0] == 'S' || query[0] == 's') {
    // execution_query.push_back(query);
    std::cout << "CONVERT TIME: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                       start)
                     .count()
              << std::endl;
    // }

    return res;
  }

  std::string trim(const std::string &s) {
    auto wsfront =
        find_if_not(s.begin(), s.end(), [](int c) { return isspace(c); });
    auto wsback = find_if_not(s.rbegin(), s.rend(), [](int c) {
                    return isspace(c);
                  }).base();
    return (wsback <= wsfront ? std::string() : std::string(wsfront, wsback));
  }

  std::string resource_folder;
  std::string output_folder;

  GraphSchema graph_schema;
  IndexManager graph_index_manager;

  GOptOfflineOptimizer gopt;
};

} // namespace relgo

#endif