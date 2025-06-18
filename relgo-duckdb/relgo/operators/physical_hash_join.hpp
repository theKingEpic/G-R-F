#ifndef PHYSICAL_HASH_JOIN_H
#define PHYSICAL_HASH_JOIN_H

#include "physical_join.hpp"

namespace relgo {

enum class HashJoinType { LEFT_JOIN, RIGHT_JOIN, INNER_JOIN };

class PhysicalHashJoin : public PhysicalJoin {
public:
  PhysicalHashJoin(HashJoinType _join_type = HashJoinType::INNER_JOIN)
      : PhysicalJoin(PhysicalOperatorType::PHYSICAL_HASH_JOIN_TYPE),
        join_type(_join_type) {};

  ~PhysicalHashJoin() {}

  HashJoinType join_type;
};

} // namespace relgo

#endif