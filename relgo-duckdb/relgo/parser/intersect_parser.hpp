#ifndef INTERSECT_PARSER_HPP
#define INTERSECT_PARSER_HPP

#include "../operators/physical_adj_join.hpp"
#include "../operators/physical_extend_intersect.hpp"
#include "../operators/physical_hash_join.hpp"
#include "../operators/physical_join.hpp"
#include "../operators/physical_scan.hpp"
#include "parse_info.hpp"

namespace relgo {

class IntersectParser {
public:
  IntersectParser() {}
  static bool opHasNullChild(std::unique_ptr<PhysicalOperator> &op);
  static std::unique_ptr<PhysicalOperator>
  parseIntersect(const physical::Intersect &intersect,
                 std::unique_ptr<PhysicalOperator> right_op,
                 ParseInfo &parse_info);
  static std::unique_ptr<PhysicalOperator>
  unpackIntersect(std::unique_ptr<PhysicalExtendIntersectJoin> &op,
                  std::unique_ptr<PhysicalOperator> right_op,
                  ParseInfo &parse_info);
  static std::unique_ptr<PhysicalOperator>
  traverseSubOperators(std::unique_ptr<PhysicalOperator> &sub_op,
                       std::unique_ptr<PhysicalOperator> &last_op,
                       idx_t &end_alias, int depth, int op_index,
                       ParseInfo &parse_info);
};

} // namespace relgo

#endif