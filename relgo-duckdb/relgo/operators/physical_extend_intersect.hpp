#ifndef PHYSICAL_EXTEND_INTERSEC_JOIN_HPP
#define PHYSICAL_EXTEND_INTERSEC_JOIN_HPP

#include "physical_join.hpp"

namespace relgo {

class PhysicalExtendIntersectJoin : public PhysicalJoin {
public:
  PhysicalExtendIntersectJoin()
      : PhysicalJoin(
            PhysicalOperatorType::PHYSICAL_EXTEND_INTERSECT_JOIN_TYPE) {
    conditions.clear();
  };

  ~PhysicalExtendIntersectJoin() {}

  std::vector<std::unique_ptr<PhysicalOperator>> sub_operators;
  std::vector<std::unique_ptr<ConditionNode>> merge_conditions;
};

} // namespace relgo

#endif