#ifndef GRAPH_OPTIMIZER_H
#define GRAPH_OPTIMIZER_H

#include <iostream>
#include <string>

#include "../proto_generated/physical.pb.h"

namespace relgo {

class GraphOptimizer {
public:
  virtual physical::PhysicalPlan optimize(std::string query) = 0;
};

} // namespace relgo

#endif