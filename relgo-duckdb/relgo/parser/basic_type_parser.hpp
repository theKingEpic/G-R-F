#ifndef BASIC_TYPE_PARSER_H
#define BASIC_TYPE_PARSER_H

#include "../graph/graph_schema.hpp"
#include "../proto_generated/common.pb.h"
#include "../proto_generated/physical.pb.h"
#include "../proto_generated/type.pb.h"
#include "../utils/types.hpp"
#include "../utils/value.hpp"
#include "meta_data_info.hpp"
#include "parse_info.hpp"

namespace relgo {

class BasicTypeParser {
public:
  static std::unique_ptr<std::vector<MetaDataInfo>>
  parseDataType(const common::DataType &item, ParseInfo &parse_info);
};

} // namespace relgo

#endif