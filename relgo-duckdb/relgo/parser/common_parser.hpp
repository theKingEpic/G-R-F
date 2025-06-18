#ifndef COMMON_PARSER_H
#define COMMON_PARSER_H

#include "../proto_generated/common.pb.h"
#include "../proto_generated/physical.pb.h"
#include "../utils/types.hpp"
#include "../utils/value.hpp"

namespace relgo {

class CommonParser {
public:
  static Value parseNameOrId(const common::NameOrId &item);
  static Value parseValue(const common::Value &item);
};

} // namespace relgo

#endif