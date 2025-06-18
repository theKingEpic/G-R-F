#include "common_parser.hpp"

#include <iostream>
#include <string>

namespace relgo {

Value CommonParser::parseNameOrId(const common::NameOrId &item) {
  switch (item.item_case()) {   // 使用 item_case() 判断当前字段
  case common::NameOrId::kName: // 对应 proto 定义中的 name = 1
    return Value(item.name());
  case common::NameOrId::kId: // 对应 proto 定义中的 id = 2
    return Value::INTEGER(item.id());
  default: // 处理未设置或未知类型
    std::cerr << "No valid item in NameOrId" << std::endl;
  }

  return Value();
}

Value CommonParser::parseValue(const common::Value &value) {
  switch (value.item_case()) {
  case common::Value::kBoolean:
    return Value::BOOLEAN(value.boolean());
  case common::Value::kI32:
    return Value::INTEGER(value.i32());
  case common::Value::kI64:
    return Value::BIGINT(value.i64());
  case common::Value::kF64:
    return Value::DOUBLE(value.f64());
  case common::Value::kStr:
    return Value(value.str());
  default:
    std::cerr << "Unknown Value type" << std::endl;
    return Value();
  }
}

} // namespace relgo
