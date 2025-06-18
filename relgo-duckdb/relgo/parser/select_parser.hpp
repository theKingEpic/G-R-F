#ifndef SELECT_PARSER_H
#define SELECT_PARSER_H

#include "../operators/physical_select.hpp"
#include "../proto_generated/algebra.pb.h"
#include "parse_info.hpp"

namespace relgo {

class SelectParser {
public:
  SelectParser() {}
  static std::unique_ptr<PhysicalSelect>
  parseSelect(const algebra::Select &select,
              std::unique_ptr<PhysicalOperator> child_op,
              ParseInfo &parse_info);
};

} // namespace relgo

#endif