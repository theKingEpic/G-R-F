#ifndef AGGREGATE_PARSER_HPP
#define AGGREGATE_PARSER_HPP

#include "../operators/physical_aggregate.hpp"
#include "parse_info.hpp"

namespace relgo {

class AggregateParser {
public:
  static std::unique_ptr<PhysicalAggregate>
  parseAggregate(const physical::GroupBy &op,
                 std::unique_ptr<PhysicalOperator> child_op,
                 ParseInfo &parse_info);
  static AggregateType convertPhysicalAggTypeToAggType(
      const physical::GroupBy_AggFunc_Aggregate type);
};

} // namespace relgo

#endif