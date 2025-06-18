/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>

#include "graph/graph_schema.hpp"
#include "index/graph_index_manager.hpp"
#include "optimizer/gopt_offline_optimizer.hpp"
#include "parser/core_parser.hpp"
#include "parser/edge_expand_parser.hpp"
#include "parser/meta_data_info.hpp"
#include "parser/scan_parser.hpp"
#include "parser/type_parser.hpp"
#include "proto_generated/physical.pb.h"
#include "utils/files.hpp"

int main(int argc, char **argv) {

  std::string graph_schema_path = "resource/graph-planner-jni/conf/graph.yaml";

  GOptOfflineOptimizer gopt("resource/graph-planner-jni/bin/test_graph_planner",
                            "resource/graph-planner-jni/libs",
                            "resource/graph-planner-jni/native",
                            "bin_file/test.bin");

  gopt.setGraphInfo(
      graph_schema_path,
      "resource/graph-planner-jni/conf/modern_statistics.json",
      "resource/graph-planner-jni/conf/gs_interactive_hiactor.yaml");

  gopt.optimize(
      "MATCH (n0:person)-[:knows]->(n1:person)-[:created]-(n2:software) WHERE "
      "50 < n1.id "
      "AND n1.id < 100 "
      "RETURN n1.name, n2.name;");

  // Check if the correct number of arguments is provided
  //   if (argc != 2) {
  //     std::cerr << "Usage: " << argv[0] << std::endl;
  //     return 1;
  //   }

  // auto start = std::chrono::high_resolution_clock::now();

  std::string plan_path = "bin_file/test.bin"; // argv[1];
  check_path_exits(plan_path);

  physical::PhysicalPlan physical_plan;
  std::ifstream input(plan_path, std::ios::binary);
  if (!physical_plan.ParseFromIstream(&input)) {
    std::cerr << "Failed to read physical plan." << std::endl;
    return -1;
  }

  GraphSchema graph_schema(graph_schema_path);
  IndexManager index_manager(
      "resource/graph-planner-jni/conf/graph_connection.yaml");
  index_manager.initFromPb(physical_plan);
  ParseInfo parse_info(graph_schema, index_manager);

  CoreParser core_parser;
  std::unique_ptr<relgo::PhysicalOperator> result =
      move(core_parser.parse(physical_plan, parse_info));
  std::cout << result->toString() << std::endl;

  return 0;
}