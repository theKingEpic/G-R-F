#ifndef CORE_PARSER_HPP
#define CORE_PARSER_HPP

#include "../operators/physical_join.hpp"
#include "../operators/physical_scan.hpp"
#include "aggregate_parser.hpp"
#include "edge_expand_parser.hpp"
#include "getv_parser.hpp"
#include "intersect_parser.hpp"
#include "parse_info.hpp"
#include "project_parser.hpp"
#include "scan_parser.hpp"
#include "select_parser.hpp"

namespace relgo {

class CoreParser {
public:
  static std::unique_ptr<PhysicalOperator>
  parse(const physical::PhysicalPlan &plan, ParseInfo &parse_info);
};

} // namespace relgo

#endif