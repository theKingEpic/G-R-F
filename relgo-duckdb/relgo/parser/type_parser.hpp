#ifndef TYPE_PARSER_H
#define TYPE_PARSER_H

#include "../graph/graph_schema.hpp"
#include "../proto_generated/common.pb.h"
#include "../proto_generated/physical.pb.h"
#include "../proto_generated/type.pb.h"
#include "../utils/types.hpp"
#include "../utils/value.hpp"
#include "basic_type_parser.hpp"
#include "common_parser.hpp"
#include "meta_data_info.hpp"
#include "parse_info.hpp"

#include <vector>

namespace relgo {

class TypeParser {
public:
  static std::unique_ptr<std::vector<MetaDataInfo>>
  parseGraphDataType(const common::GraphDataType &item, ParseInfo &parse_info);
};

} // namespace relgo

#endif