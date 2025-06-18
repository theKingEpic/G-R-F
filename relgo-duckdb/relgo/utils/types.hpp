#ifndef TYPES_HPP
#define TYPES_HPP

#include "../proto_generated/basic_type.pb.h"
#include "functions.hpp"

#include <iostream>
#include <vector>

namespace relgo {

typedef uint64_t idx_t;
typedef int64_t row_t;
typedef idx_t column_t;
typedef std::vector<bool> bitmask_vector;
typedef std::vector<row_t> rows_vector;

#define STANDARD_VECTOR_SIZE 2048
const column_t COLUMN_IDENTIFIER_GRAPH_INDEX_ID = (column_t)100;

typedef unsigned char uint8_t;
using union_tag_t = uint8_t;
template <class T> using child_list_t = std::vector<std::pair<std::string, T>>;

enum class PhysicalType : uint8_t {
  ///// A NULL type having no physical storage
  // NA = 0,

  /// Boolean as 8 bit "bool" value
  BOOL = 1,

  /// Unsigned 8-bit little-endian integer
  UINT8 = 2,

  /// Signed 8-bit little-endian integer
  INT8 = 3,

  /// Unsigned 16-bit little-endian integer
  UINT16 = 4,

  /// Signed 16-bit little-endian integer
  INT16 = 5,

  /// Unsigned 32-bit little-endian integer
  UINT32 = 6,

  /// Signed 32-bit little-endian integer
  INT32 = 7,

  /// Unsigned 64-bit little-endian integer
  UINT64 = 8,

  /// Signed 64-bit little-endian integer
  INT64 = 9,

  ///// 2-byte floating point value
  // HALF_FLOAT = 10,

  /// 4-byte floating point value
  FLOAT = 11,

  /// 8-byte floating point value
  DOUBLE = 12,

  ///// UTF8 variable-length string as List<Char>
  // STRING = 13,

  ///// Variable-length bytes (no guarantee of UTF8-ness)
  // BINARY = 14,

  ///// Fixed-size binary. Each value occupies the same number of bytes
  // FIXED_SIZE_BINARY = 15,

  ///// int32_t days since the UNIX epoch
  // DATE32 = 16,

  ///// int64_t milliseconds since the UNIX epoch
  // DATE64 = 17,

  ///// Exact timestamp encoded with int64 since UNIX epoch
  ///// Default unit millisecond
  // TIMESTAMP = 18,

  ///// Time as signed 32-bit integer, representing either seconds or
  ///// milliseconds since midnight
  // TIME32 = 19,

  ///// Time as signed 64-bit integer, representing either microseconds or
  ///// nanoseconds since midnight
  // TIME64 = 20,

  /// YEAR_MONTH or DAY_TIME interval in SQL style
  INTERVAL = 21,

  /// Precision- and scale-based decimal type. Storage type depends on the
  /// parameters.
  // DECIMAL = 22,

  /// A list of some logical data type
  LIST = 23,

  /// Struct of logical types
  STRUCT = 24,

  ///// Unions of logical types
  // UNION = 25,

  ///// Dictionary-encoded type, also called "categorical" or "factor"
  ///// in other programming languages. Holds the dictionary value
  ///// type but not the dictionary itself, which is part of the
  ///// ArrayData struct
  // DICTIONARY = 26,

  ///// Custom data type, implemented by user
  // EXTENSION = 28,

  ///// Fixed size list of some logical type
  // FIXED_SIZE_LIST = 29,

  ///// Measure of elapsed time in either seconds, milliseconds, microseconds
  ///// or nanoseconds.
  // DURATION = 30,

  ///// Like STRING, but with 64-bit offsets
  // LARGE_STRING = 31,

  ///// Like BINARY, but with 64-bit offsets
  // LARGE_BINARY = 32,

  ///// Like LIST, but with 64-bit offsets
  // LARGE_LIST = 33,

  /// DuckDB Extensions
  VARCHAR = 200, // our own string representation, different from STRING and
                 // LARGE_STRING above
  INT128 = 204,  // 128-bit integers
  UNKNOWN = 205, // Unknown physical type of user defined types
  /// Boolean as 1 bit, LSB bit-packed ordering
  BIT = 206,
  POINTER = 207,
  HASH = 208,

  INVALID = 255
};

inline PhysicalType stringToPhysicalType(std::string &str) {
  if (inString(str, "BOOL")) {
    return PhysicalType::BOOL;
  } else if (inString(str, "UINT8")) {
    return PhysicalType::UINT8;
  } else if (inString(str, "INT8")) {
    return PhysicalType::INT8;
  } else if (inString(str, "UINT16")) {
    return PhysicalType::UINT16;
  } else if (inString(str, "INT16")) {
    return PhysicalType::INT16;
  } else if (inString(str, "UINT32")) {
    return PhysicalType::UINT32;
  } else if (inString(str, "INT32")) {
    return PhysicalType::INT32;
  } else if (inString(str, "UINT64")) {
    return PhysicalType::UINT64;
  } else if (inString(str, "INT64")) {
    return PhysicalType::INT64;
  } else if (inString(str, "FLOAT")) {
    return PhysicalType::FLOAT;
  } else if (inString(str, "DOUBLE")) {
    return PhysicalType::DOUBLE;
  } else if (inString(str, "STRING") || inString(str, "VARCHAR")) {
    return PhysicalType::VARCHAR;
  } else if (inString(str, "INTERVAL")) {
    return PhysicalType::INTERVAL;
  } else if (inString(str, "LIST")) {
    return PhysicalType::LIST;
  } else if (inString(str, "STRUCT")) {
    return PhysicalType::STRUCT;
  } else if (inString(str, "INT128")) {
    return PhysicalType::INT128;
  } else if (inString(str, "BIT")) {
    return PhysicalType::BIT;
  } else if (inString(str, "POINTER")) {
    return PhysicalType::POINTER;
  } else if (inString(str, "HASH")) {
    return PhysicalType::HASH;
  }

  return PhysicalType::UNKNOWN;
}

inline std::string physicalTypeToString(PhysicalType type) {
  switch (type) {
  case PhysicalType::BOOL:
    return "BOOL";
  case PhysicalType::UINT8:
    return "UINT8";
  case PhysicalType::INT8:
    return "INT8";
  case PhysicalType::UINT16:
    return "UINT16";
  case PhysicalType::INT16:
    return "INT16";
  case PhysicalType::UINT32:
    return "UINT32";
  case PhysicalType::INT32:
    return "INT32";
  case PhysicalType::UINT64:
    return "UINT64";
  case PhysicalType::INT64:
    return "INT64";
  case PhysicalType::FLOAT:
    return "FLOAT";
  case PhysicalType::DOUBLE:
    return "DOUBLE";
  case PhysicalType::INTERVAL:
    return "INTERVAL";
  case PhysicalType::LIST:
    return "LIST";
  case PhysicalType::STRUCT:
    return "STRUCT";
  case PhysicalType::VARCHAR:
    return "VARCHAR";
  case PhysicalType::INT128:
    return "INT128";
  case PhysicalType::UNKNOWN:
    return "UNKNOWN";
  case PhysicalType::BIT:
    return "BIT";
  case PhysicalType::POINTER:
    return "POINTER";
  case PhysicalType::HASH:
    return "HASH";
  case PhysicalType::INVALID:
    return "INVALID";
  default:
    return "UNKNOWN TYPE";
  }
}

inline PhysicalType pbPrimitiveTypeToPhysicalType(common::PrimitiveType type) {
  switch (type) {
  case common::PrimitiveType::DT_ANY:
    return PhysicalType::UNKNOWN;
  case common::PrimitiveType::DT_SIGNED_INT32:
    return PhysicalType::INT32;
  case common::PrimitiveType::DT_UNSIGNED_INT32:
    return PhysicalType::UINT32;
  case common::PrimitiveType::DT_SIGNED_INT64:
    return PhysicalType::INT64;
  case common::PrimitiveType::DT_UNSIGNED_INT64:
    return PhysicalType::UINT64;
  case common::PrimitiveType::DT_BOOL:
    return PhysicalType::BOOL;
  case common::PrimitiveType::DT_FLOAT:
    return PhysicalType::FLOAT;
  case common::PrimitiveType::DT_DOUBLE:
    return PhysicalType::DOUBLE;
  case common::PrimitiveType::DT_NULL:
    return PhysicalType::UNKNOWN;
  default:
    break;
  }

  return PhysicalType::UNKNOWN;
}

inline common::PrimitiveType physicalTypeToPbPrimitiveType(PhysicalType type) {
  switch (type) {
  case PhysicalType::INT32:
    return common::PrimitiveType::DT_SIGNED_INT32;
  case PhysicalType::UINT32:
    return common::PrimitiveType::DT_UNSIGNED_INT32;
  case PhysicalType::INT64:
    return common::PrimitiveType::DT_SIGNED_INT64;
  case PhysicalType::UINT64:
    return common::PrimitiveType::DT_UNSIGNED_INT64;
  case PhysicalType::BOOL:
    return common::PrimitiveType::DT_BOOL;
  case PhysicalType::FLOAT:
    return common::PrimitiveType::DT_FLOAT;
  case PhysicalType::DOUBLE:
    return common::PrimitiveType::DT_DOUBLE;
  case PhysicalType::UNKNOWN:
    return common::PrimitiveType::DT_ANY; // 或者 DT_NULL，根据您的具体需求
  default:
    break;
  }
  return common::PrimitiveType::DT_ANY; // 或者 DT_NULL，根据您的具体需求
}

} // namespace relgo

#endif