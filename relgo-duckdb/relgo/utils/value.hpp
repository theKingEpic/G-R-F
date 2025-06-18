#ifndef VALUE_HPP
#define VALUE_HPP

#include "../proto_generated/common.pb.h"
#include "../proto_generated/physical.pb.h"
#include "types.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace relgo {

struct ExtraValueInfo;

//! The Value object holds a single arbitrary value of any type that can be
//! stored in the database.
class Value {
  friend struct StringValue;
  friend struct StructValue;
  friend struct ListValue;
  friend struct UnionValue;

public:
  //! Create an empty NULL value of the specified type
  explicit Value(PhysicalType type = PhysicalType::UNKNOWN);
  //! Create an INTEGER value
  Value(int32_t val); // NOLINT: Allow implicit conversion from `int32_t`
  //! Create a BIGINT value
  Value(int64_t val); // NOLINT: Allow implicit conversion from `int64_t`
  //! Create a FLOAT value
  Value(float val); // NOLINT: Allow implicit conversion from `float`
  //! Create a DOUBLE value
  Value(double val); // NOLINT: Allow implicit conversion from `double`
  //! Create a VARCHAR value
  Value(
      const char *val); // NOLINT: Allow implicit conversion from `const char *`
  //! Create a NULL value
  Value(
      std::nullptr_t val); // NOLINT: Allow implicit conversion from `nullptr_t`
  //! Create a VARCHAR value
  Value(std::string val); // NOLINT: Allow implicit conversion from `string`
  //! Copy constructor
  Value(const Value &other);
  //! Move constructor
  Value(Value &&other) noexcept;
  //! Destructor
  ~Value();

  // copy assignment
  Value &operator=(const Value &other);
  // move assignment
  Value &operator=(Value &&other) noexcept;

  static bool FloatIsFinite(float value);
  static bool DoubleIsFinite(double value);
  template <class T> static bool IsNan(T value) {
    std::cout << "Unimplemented template type for Value::IsNan" << std::endl;
    return false;
  }
  template <class T> static bool IsFinite(T value) { return true; }

  inline PhysicalType &GetTypeMutable() { return type_; }
  inline const PhysicalType &type() const { // NOLINT
    return type_;
  }
  inline bool IsNull() const { return is_null; }

  static Value Numeric(const PhysicalType &type, int64_t value);

  //! Create a tinyint Value from a specified value
  static Value BOOLEAN(int8_t value);
  //! Create a tinyint Value from a specified value
  static Value TINYINT(int8_t value);
  //! Create a smallint Value from a specified value
  static Value SMALLINT(int16_t value);
  //! Create an integer Value from a specified value
  static Value INTEGER(int32_t value);
  //! Create a bigint Value from a specified value
  static Value BIGINT(int64_t value);
  //! Create an unsigned tinyint Value from a specified value
  static Value UTINYINT(uint8_t value);
  //! Create an unsigned smallint Value from a specified value
  static Value USMALLINT(uint16_t value);
  //! Create an unsigned integer Value from a specified value
  static Value UINTEGER(uint32_t value);
  //! Create an unsigned bigint Value from a specified value
  static Value UBIGINT(uint64_t value);
  //! Create a pointer Value from a specified value
  static Value POINTER(uintptr_t value);
  //! Create a date Value from a specified date
  static Value DATE(int32_t year, int32_t month, int32_t day);
  //! Create a time Value from a specified time
  static Value TIME(int32_t hour, int32_t min, int32_t sec, int32_t micros);
  //! Create a timestamp Value from a specified timestamp in separate values
  static Value TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour,
                         int32_t min, int32_t sec, int32_t micros);
  static Value INTERVAL(int32_t months, int32_t days, int64_t micros);

  //! Create a float Value from a specified value
  static Value FLOAT(float value);
  //! Create a double Value from a specified value
  static Value DOUBLE(double value);
  //! Create a Value from a pb common value
  static Value PbCommonValue(const common::Value &value);

  template <class T> static Value CreateValue(T value) {
    std::cout << "No specialization exists for this type" << std::endl;
    return Value(nullptr);
  }
  // Returns the internal value. Unlike GetValue(), this method does not perform
  // casting, and assumes T matches the type of the value. Only use this if you
  // know what you are doing.
  template <class T> T GetValueUnsafe() const;

  //! Return a copy of this value
  Value Copy() const { return Value(*this); }

  //! Convert this value to a string
  std::string ToString() const;

  uintptr_t GetPointer() const;

  //===--------------------------------------------------------------------===//
  // Comparison Operators
  //===--------------------------------------------------------------------===//
  bool operator==(const Value &rhs) const;
  bool operator!=(const Value &rhs) const;
  bool operator<(const Value &rhs) const;
  bool operator>(const Value &rhs) const;
  bool operator<=(const Value &rhs) const;
  bool operator>=(const Value &rhs) const;

  bool operator==(const int64_t &rhs) const;
  bool operator!=(const int64_t &rhs) const;
  bool operator<(const int64_t &rhs) const;
  bool operator>(const int64_t &rhs) const;
  bool operator<=(const int64_t &rhs) const;
  bool operator>=(const int64_t &rhs) const;

  friend std::ostream &operator<<(std::ostream &out, const Value &val) {
    out << val.ToString();
    return out;
  }

public:
  //! The logical of the value
  PhysicalType type_; // NOLINT

  //! Whether or not the value is NULL
  bool is_null;

  //! The value of the object, if it is of a constant size Type
  union Val {
    int8_t boolean;
    int8_t tinyint;
    int16_t smallint;
    int32_t integer;
    int64_t bigint;
    uint8_t utinyint;
    uint16_t usmallint;
    uint32_t uinteger;
    uint64_t ubigint;
    float float_;   // NOLINT
    double double_; // NOLINT
    uintptr_t pointer;
    uint64_t hash;
  } value_; // NOLINT

  std::shared_ptr<ExtraValueInfo> value_info_; // NOLINT
};

//===--------------------------------------------------------------------===//
// Type-specific getters
//===--------------------------------------------------------------------===//
// Note that these are equivalent to calling GetValueUnsafe<X>, meaning no cast
// will be performed instead, an assertion will be triggered if the value is not
// of the correct type
struct BooleanValue {
  static bool Get(const Value &value);
};

struct TinyIntValue {
  static int8_t Get(const Value &value);
};

struct SmallIntValue {
  static int16_t Get(const Value &value);
};

struct IntegerValue {
  static int32_t Get(const Value &value);
};

struct BigIntValue {
  static int64_t Get(const Value &value);
};

struct UTinyIntValue {
  static uint8_t Get(const Value &value);
};

struct USmallIntValue {
  static uint16_t Get(const Value &value);
};

struct UIntegerValue {
  static uint32_t Get(const Value &value);
};

struct UBigIntValue {
  static uint64_t Get(const Value &value);
};

struct FloatValue {
  static float Get(const Value &value);
};

struct DoubleValue {
  static double Get(const Value &value);
};

struct StringValue {
  static const std::string &Get(const Value &value);
};

struct StructValue {
  static const std::vector<Value> &GetChildren(const Value &value);
};

struct ListValue {
  static const std::vector<Value> &GetChildren(const Value &value);
};

struct UnionValue {
  static const Value &GetValue(const Value &value);
  static uint8_t GetTag(const Value &value);
  static const PhysicalType &GetType(const Value &value);
};

template <> Value Value::CreateValue(bool value);
template <> Value Value::CreateValue(uint8_t value);
template <> Value Value::CreateValue(uint16_t value);
template <> Value Value::CreateValue(uint32_t value);
template <> Value Value::CreateValue(uint64_t value);
template <> Value Value::CreateValue(int8_t value);
template <> Value Value::CreateValue(int16_t value);
template <> Value Value::CreateValue(int32_t value);
template <> Value Value::CreateValue(int64_t value);
template <> Value Value::CreateValue(const char *value);
template <> Value Value::CreateValue(std::string value);
template <> Value Value::CreateValue(float value);
template <> Value Value::CreateValue(double value);
template <> Value Value::CreateValue(Value value);

template <> bool Value::GetValueUnsafe() const;
template <> int8_t Value::GetValueUnsafe() const;
template <> int16_t Value::GetValueUnsafe() const;
template <> int32_t Value::GetValueUnsafe() const;
template <> int64_t Value::GetValueUnsafe() const;
template <> uint8_t Value::GetValueUnsafe() const;
template <> uint16_t Value::GetValueUnsafe() const;
template <> uint32_t Value::GetValueUnsafe() const;
template <> uint64_t Value::GetValueUnsafe() const;
template <> std::string Value::GetValueUnsafe() const;
template <> float Value::GetValueUnsafe() const;
template <> double Value::GetValueUnsafe() const;

} // namespace relgo

#endif