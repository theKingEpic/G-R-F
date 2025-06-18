#ifndef GETV_PARSER_HPP
#define GETV_PARSER_HPP

#include "../operators/physical_adj_join.hpp"
#include "../operators/physical_hash_join.hpp"
#include "../operators/physical_join.hpp"
#include "../operators/physical_scan.hpp"
#include "parse_info.hpp"

namespace relgo {

class GetVParser {
public:
  GetVParser() {}

  static std::unique_ptr<PhysicalJoin>
  parseGetV(const physical::GetV &vertex,
            std::unique_ptr<PhysicalOperator> right_op, ParseInfo &parse_info);
  static bool isScanRight(const physical::GetV &vertex, std::string &table_id,
                          ParseInfo &parse_info);
};

} // namespace relgo

#endif