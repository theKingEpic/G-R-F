#ifndef PHYSICAL_AGGREGATE_HPP
#define PHYSICAL_AGGREGATE_HPP

#include "../parser/expr_parser.hpp"
#include "condition_tree.hpp"
#include "physical_operator.hpp"
#include <sstream>
#include <string>
#include <vector>

namespace relgo {

enum class AggregateType {
  NULL_TYPE,
  COUNT_TYPE,
  COUNT_DISTINCT_TYPE,
  MAX_TYPE,
  MIN_TYPE,
  SUM_TYPE
};

inline std::string convertAggTypeToStr(AggregateType &type) {
  switch (type) {
  case AggregateType::MAX_TYPE:
    return "max";
  case AggregateType::MIN_TYPE:
    return "min";
  case AggregateType::COUNT_TYPE:
    return "count";
  case AggregateType::COUNT_DISTINCT_TYPE:
    return "count_distinct";
  case AggregateType::SUM_TYPE:
    return "sum";
  default:
    return "";
  }
  return "";
}

class PhysicalAggregate : public PhysicalOperator {
public:
  PhysicalAggregate()
      : PhysicalOperator(PhysicalOperatorType::PHYSICAL_AGGREGATE_TYPE) {}
  PhysicalAggregate(std::vector<std::shared_ptr<VariableExprOpr>> &_groupby,
                    std::vector<AggregateType> _aggregate_type_list)
      : PhysicalOperator(PhysicalOperatorType::PHYSICAL_AGGREGATE_TYPE),
        groupby(move(_groupby)), aggregate_type_list(_aggregate_type_list) {}
  ~PhysicalAggregate() {}

  std::string toString();

public:
  std::unique_ptr<PhysicalOperator> child;
  std::vector<std::shared_ptr<VariableExprOpr>> groupby;
  std::vector<AggregateType> aggregate_type_list;
};

} // namespace relgo

#endif