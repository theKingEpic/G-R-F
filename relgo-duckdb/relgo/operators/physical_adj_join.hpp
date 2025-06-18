#ifndef PHYSICAL_ADJ_JOIN_H
#define PHYSICAL_ADJ_JOIN_H

#include "physical_join.hpp"

namespace relgo {

class PhysicalAdjJoin : public PhysicalJoin {
public:
  PhysicalAdjJoin()
      : PhysicalJoin(PhysicalOperatorType::PHYSICAL_ADJ_JOIN_TYPE){};

  ~PhysicalAdjJoin() {}
};

} // namespace relgo

#endif