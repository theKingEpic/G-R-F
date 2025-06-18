#ifndef GRAPH_OFFLINE_OPTIMIZER_H
#define GRAPH_OFFLINE_OPTIMIZER_H

#include <iostream>
#include <string>

#include "graph_optimizer.hpp"
#include "../proto_generated/physical.pb.h"

namespace relgo {

class GraphOfflineOptimizer : public GraphOptimizer {
public:
  GraphOfflineOptimizer(std::string _binary_path, std::string _output_path)
      : binary_path(_binary_path), output_path(_output_path) {}

  virtual physical::PhysicalPlan optimize(std::string query) override {
    std::cout << "optimize with offline optimizer" << std::endl;
  }

  std::string binary_path;
  std::string output_path;
};

} // namespace relgo

#endif