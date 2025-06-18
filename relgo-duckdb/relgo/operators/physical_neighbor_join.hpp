#ifndef PHYSICAL_NEIGHBOR_JOIN_H
#define PHYSICAL_NEIGHBOR_JOIN_H

#include "physical_join.hpp"

namespace relgo {

class PhysicalNeighborJoin : public PhysicalJoin {
public:
  PhysicalNeighborJoin()
      : PhysicalJoin(PhysicalOperatorType::PHYSICAL_NEIGHBOR_JOIN_TYPE) {};

  ~PhysicalNeighborJoin() {}
};

} // namespace relgo

#endif