#ifndef PHYSICAL_JOIN_H
#define PHYSICAL_JOIN_H

#include "../graph/graph_schema.hpp"
#include "condition_tree.hpp"
#include "physical_operator.hpp"
#include <sstream>
#include <string>

namespace relgo {

enum class JoinDirection {
  UNKNOWN,
  EDGE_SOURCE,
  EDGE_TARGET,
  TARGET_EDGE,
  SOURCE_EDGE
};

enum class JoinIndexType {
  UNKNOWN,
  HASH_JOIN,
  ADJ_JOIN,
  NEIGHBOR_JOIN,
  EXTEND_INTERSECT_JOIN
};

inline JoinDirection fromStrToJoinDirection(std::string str) {
  if (str == "edge_source")
    return JoinDirection::EDGE_SOURCE;
  else if (str == "edge_target")
    return JoinDirection::EDGE_TARGET;
  else if (str == "target_edge")
    return JoinDirection::TARGET_EDGE;
  else if (str == "source_edge")
    return JoinDirection::SOURCE_EDGE;
  else
    return JoinDirection::UNKNOWN;
}

inline JoinDirection reverseJoinDirection(JoinDirection dir) {
  if (dir == JoinDirection::EDGE_SOURCE)
    return JoinDirection::SOURCE_EDGE;
  else if (dir == JoinDirection::EDGE_TARGET)
    return JoinDirection::TARGET_EDGE;
  else if (dir == JoinDirection::TARGET_EDGE)
    return JoinDirection::EDGE_TARGET;
  else if (dir == JoinDirection::SOURCE_EDGE)
    return JoinDirection::EDGE_SOURCE;
  else
    return JoinDirection::UNKNOWN;
}

class PhysicalJoin : public PhysicalOperator {
public:
  PhysicalJoin(PhysicalOperator op = PhysicalOperatorType::PHYSICAL_JOIN_TYPE)
      : PhysicalOperator(op), use_graph_index(false) {}
  PhysicalJoin(std::unique_ptr<PhysicalOperator> _left,
               std::unique_ptr<PhysicalOperator> _right,
               std::vector<std::unique_ptr<ConditionNode>> _conditions,
               PhysicalOperator op = PhysicalOperatorType::PHYSICAL_JOIN_TYPE)
      : PhysicalOperator(op), left(move(_left)), right(move(_right)),
        use_graph_index(false) {
    for (auto &node : _conditions) {
      conditions.push_back(move(node));
    }
  }
  ~PhysicalJoin() {}

  std::string toString();

public:
  std::unique_ptr<PhysicalOperator> left;
  std::unique_ptr<PhysicalOperator> right;
  std::vector<std::unique_ptr<ConditionNode>> conditions;
  bool use_graph_index;
  JoinDirection join_direction;
  std::vector<relgo::Property> output_info;

  std::string edge_table_name;
};

} // namespace relgo

#endif