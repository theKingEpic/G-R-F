#include "value.hpp"
#include "value_operations.hpp"

#include <cmath>

namespace relgo {

enum class ExtraValueInfoType : uint8_t {
  INVALID_TYPE_INFO = 0,
  STRING_VALUE_INFO = 1,
  NESTED_VALUE_INFO = 2
};

struct ExtraValueInfo {
  explicit ExtraValueInfo(ExtraValueInfoType type) : type(type) {}
  virtual ~ExtraValueInfo() {}

  ExtraValueInfoType type;

public:
  bool Equals(ExtraValueInfo *other_p) const {
    if (!other_p) {
      return false;
    }
    if (type != other_p->type) {
      return false;
    }
    return EqualsInternal(other_p);
  }

  template <class T> T &Get() {
    if (type != T::TYPE) {
      std::cout << "ExtraValueInfo type mismatch" << std::endl;
    }
    return (T &)*this;
  }

protected:
  virtual bool EqualsInternal(ExtraValueInfo *other_p) const { return true; }
};

//===--------------------------------------------------------------------===//
// String Value Info
//===--------------------------------------------------------------------===//
struct StringValueInfo : public ExtraValueInfo {
  static constexpr const ExtraValueInfoType TYPE =
      ExtraValueInfoType::STRING_VALUE_INFO;

public:
  explicit StringValueInfo(std::string str_p)
      : ExtraValueInfo(ExtraValueInfoType::STRING_VALUE_INFO),
        str(std::move(str_p)) {}

  const std::string &GetString() { return str; }

protected:
  bool EqualsInternal(ExtraValueInfo *other_p) const override {
    return other_p->Get<StringValueInfo>().str == str;
  }

  std::string str;
};

//===--------------------------------------------------------------------===//
// Nested Value Info
//===--------------------------------------------------------------------===//
struct NestedValueInfo : public ExtraValueInfo {
  static constexpr const ExtraValueInfoType TYPE =
      ExtraValueInfoType::NESTED_VALUE_INFO;

public:
  NestedValueInfo() : ExtraValueInfo(ExtraValueInfoType::NESTED_VALUE_INFO) {}
  explicit NestedValueInfo(std::vector<Value> values_p)
      : ExtraValueInfo(ExtraValueInfoType::NESTED_VALUE_INFO),
        values(std::move(values_p)) {}

  const std::vector<Value> &GetValues() { return values; }

protected:
  bool EqualsInternal(ExtraValueInfo *other_p) const override {
    return other_p->Get<NestedValueInfo>().values == values;
  }

  std::vector<Value> values;
};
//===--------------------------------------------------------------------===//
// Value
//===--------------------------------------------------------------------===//
Value::Value(PhysicalType type) : type_(std::move(type)), is_null(true) {}

Value::Value(int32_t val) : type_(PhysicalType::INT32), is_null(false) {
  value_.integer = val;
}

Value::Value(int64_t val) : type_(PhysicalType::INT64), is_null(false) {
  value_.bigint = val;
}

Value::Value(float val) : type_(PhysicalType::FLOAT), is_null(false) {
  value_.float_ = val;
}

Value::Value(double val) : type_(PhysicalType::DOUBLE), is_null(false) {
  value_.double_ = val;
}

Value::Value(const char *val) : Value(val ? std::string(val) : std::string()) {}

Value::Value(std::nullptr_t val) : Value(PhysicalType::VARCHAR) {}

Value::Value(std::string val) : type_(PhysicalType::VARCHAR), is_null(false) {
  //   if (!Value::StringIsValid(val.c_str(), val.size())) {
  //     std::cout << "Invalid construction" << std::endl;
  //     // throw Exception(
  //     //     ErrorManager::InvalidUnicodeError(val, "value construction"));
  //   }
  value_info_ = std::make_shared<StringValueInfo>(std::move(val));
}

Value::~Value() {}

Value::Value(const Value &other)
    : type_(other.type_), is_null(other.is_null), value_(other.value_),
      value_info_(other.value_info_) {}

Value::Value(Value &&other) noexcept
    : type_(std::move(other.type_)), is_null(other.is_null),
      value_(other.value_), value_info_(std::move(other.value_info_)) {}

Value &Value::operator=(const Value &other) {
  if (this == &other) {
    return *this;
  }
  type_ = other.type_;
  is_null = other.is_null;
  value_ = other.value_;
  value_info_ = other.value_info_;
  return *this;
}

Value &Value::operator=(Value &&other) noexcept {
  type_ = std::move(other.type_);
  is_null = other.is_null;
  value_ = other.value_;
  value_info_ = std::move(other.value_info_);
  return *this;
}

Value Value::BOOLEAN(int8_t value) {
  Value result(PhysicalType::BOOL);
  result.value_.boolean = bool(value);
  result.is_null = false;
  return result;
}

Value Value::TINYINT(int8_t value) {
  Value result(PhysicalType::INT8);
  result.value_.tinyint = value;
  result.is_null = false;
  return result;
}

Value Value::SMALLINT(int16_t value) {
  Value result(PhysicalType::INT16);
  result.value_.smallint = value;
  result.is_null = false;
  return result;
}

Value Value::INTEGER(int32_t value) {
  Value result(PhysicalType::INT32);
  result.value_.integer = value;
  result.is_null = false;
  return result;
}

Value Value::BIGINT(int64_t value) {
  Value result(PhysicalType::INT64);
  result.value_.bigint = value;
  result.is_null = false;
  return result;
}

Value Value::UTINYINT(uint8_t value) {
  Value result(PhysicalType::UINT8);
  result.value_.utinyint = value;
  result.is_null = false;
  return result;
}

Value Value::USMALLINT(uint16_t value) {
  Value result(PhysicalType::UINT16);
  result.value_.usmallint = value;
  result.is_null = false;
  return result;
}

Value Value::UINTEGER(uint32_t value) {
  Value result(PhysicalType::UINT32);
  result.value_.uinteger = value;
  result.is_null = false;
  return result;
}

Value Value::UBIGINT(uint64_t value) {
  Value result(PhysicalType::UINT64);
  result.value_.ubigint = value;
  result.is_null = false;
  return result;
}

Value Value::FLOAT(float value) {
  Value result(PhysicalType::FLOAT);
  result.value_.float_ = value;
  result.is_null = false;
  return result;
}

Value Value::DOUBLE(double value) {
  Value result(PhysicalType::DOUBLE);
  result.value_.double_ = value;
  result.is_null = false;
  return result;
}

Value Value::POINTER(uintptr_t value) {
  Value result(PhysicalType::POINTER);
  result.value_.pointer = value;
  result.is_null = false;
  return result;
}

Value Value::PbCommonValue(const common::Value &val) {
  switch (val.item_case()) {
  case common::Value::kBoolean:
    return Value::BOOLEAN(val.boolean());
  case common::Value::kI32:
    return Value::INTEGER(val.i32());
  case common::Value::kI64:
    return Value::BIGINT(val.i64());
  case common::Value::kF64:
    return Value::DOUBLE(val.f64());
  case common::Value::kStr:
    return Value(val.str());
  default:
    std::cout << "Value convertion unsupported";
  }

  return Value();
}

bool Value::FloatIsFinite(float value) {
  return !(std::isnan(value) || std::isinf(value));
}

bool Value::DoubleIsFinite(double value) {
  return !(std::isnan(value) || std::isinf(value));
}

template <> bool Value::IsNan(float input) { return std::isnan(input); }

template <> bool Value::IsNan(double input) { return std::isnan(input); }

//===--------------------------------------------------------------------===//
// CreateValue
//===--------------------------------------------------------------------===//
template <> Value Value::CreateValue(bool value) {
  return Value::BOOLEAN(value);
}

template <> Value Value::CreateValue(int8_t value) {
  return Value::TINYINT(value);
}

template <> Value Value::CreateValue(int16_t value) {
  return Value::SMALLINT(value);
}

template <> Value Value::CreateValue(int32_t value) {
  return Value::INTEGER(value);
}

template <> Value Value::CreateValue(int64_t value) {
  return Value::BIGINT(value);
}

template <> Value Value::CreateValue(uint8_t value) {
  return Value::UTINYINT(value);
}

template <> Value Value::CreateValue(uint16_t value) {
  return Value::USMALLINT(value);
}

template <> Value Value::CreateValue(uint32_t value) {
  return Value::UINTEGER(value);
}

template <> Value Value::CreateValue(uint64_t value) {
  return Value::UBIGINT(value);
}

template <> Value Value::CreateValue(const char *value) {
  return Value(std::string(value));
}

template <> Value Value::CreateValue(float value) {
  return Value::FLOAT(value);
}

template <> Value Value::CreateValue(double value) {
  return Value::DOUBLE(value);
}

template <> Value Value::CreateValue(Value value) { return value; }

uintptr_t Value::GetPointer() const { return value_.pointer; }

Value Value::Numeric(const PhysicalType &type, int64_t value) {
  switch (type) {
  case PhysicalType::BOOL:
    return Value::BOOLEAN(value ? 1 : 0);
  case PhysicalType::INT8:
    return Value::TINYINT((int8_t)value);
  case PhysicalType::INT16:
    return Value::SMALLINT((int16_t)value);
  case PhysicalType::INT32:
    return Value::INTEGER((int32_t)value);
  case PhysicalType::INT64:
    return Value::BIGINT(value);
  case PhysicalType::UINT8:
    return Value::UTINYINT((uint8_t)value);
  case PhysicalType::UINT16:
    return Value::USMALLINT((uint16_t)value);
  case PhysicalType::UINT32:
    return Value::UINTEGER((uint32_t)value);
  case PhysicalType::UINT64:
    return Value::UBIGINT(value);
  case PhysicalType::FLOAT:
    return Value((float)value);
  case PhysicalType::DOUBLE:
    return Value((double)value);
  case PhysicalType::POINTER:
    return Value::POINTER(value);
  default:
    std::cout << "Numeric requires numeric type" << std::endl;
  }
}

//===--------------------------------------------------------------------===//
// GetValueUnsafe
//===--------------------------------------------------------------------===//
template <> bool Value::GetValueUnsafe() const {
  //   D_ASSERT(type_.InternalType() == PhysicalType::BOOL);
  return value_.boolean;
}

template <> int8_t Value::GetValueUnsafe() const {
  //   D_ASSERT(type_.InternalType() == PhysicalType::INT8 ||
  //            type_.InternalType() == PhysicalType::BOOL);
  return value_.tinyint;
}

template <> int16_t Value::GetValueUnsafe() const {
  //   D_ASSERT(type_.InternalType() == PhysicalType::INT16);
  return value_.smallint;
}

template <> int32_t Value::GetValueUnsafe() const {
  //   D_ASSERT(type_.InternalType() == PhysicalType::INT32);
  return value_.integer;
}

template <> int64_t Value::GetValueUnsafe() const {
  //   D_ASSERT(type_.InternalType() == PhysicalType::INT64);
  return value_.bigint;
}

template <> uint8_t Value::GetValueUnsafe() const {
  //   D_ASSERT(type_.InternalType() == PhysicalType::UINT8);
  return value_.utinyint;
}

template <> uint16_t Value::GetValueUnsafe() const {
  //   D_ASSERT(type_.InternalType() == PhysicalType::UINT16);
  return value_.usmallint;
}

template <> uint32_t Value::GetValueUnsafe() const {
  //   D_ASSERT(type_.InternalType() == PhysicalType::UINT32);
  return value_.uinteger;
}

template <> uint64_t Value::GetValueUnsafe() const {
  //   D_ASSERT(type_.InternalType() == PhysicalType::UINT64);
  return value_.ubigint;
}

template <> std::string Value::GetValueUnsafe() const {
  return StringValue::Get(*this);
}

template <> float Value::GetValueUnsafe() const {
  //   D_ASSERT(type_.InternalType() == PhysicalType::FLOAT);
  return value_.float_;
}

template <> double Value::GetValueUnsafe() const {
  //   D_ASSERT(type_.InternalType() == PhysicalType::DOUBLE);
  return value_.double_;
}

std::string Value::ToString() const {
  if (IsNull()) {
    return "NULL";
  }

  PhysicalType pt = this->type();
  switch (pt) {
  case PhysicalType::VARCHAR:
    return this->GetValueUnsafe<std::string>();
  case PhysicalType::BOOL:
    return std::to_string(this->GetValueUnsafe<bool>());
  case PhysicalType::INT8:
    return std::to_string(this->GetValueUnsafe<int8_t>());
  case PhysicalType::INT16:
    return std::to_string(this->GetValueUnsafe<int16_t>());
  case PhysicalType::INT32:
    return std::to_string(this->GetValueUnsafe<int32_t>());
  case PhysicalType::INT64:
    return std::to_string(this->GetValueUnsafe<int64_t>());
  case PhysicalType::UINT8:
    return std::to_string(this->GetValueUnsafe<uint8_t>());
  case PhysicalType::UINT16:
    return std::to_string(this->GetValueUnsafe<uint16_t>());
  case PhysicalType::UINT32:
    return std::to_string(this->GetValueUnsafe<uint32_t>());
  case PhysicalType::UINT64:
    return std::to_string(this->GetValueUnsafe<uint64_t>());
  case PhysicalType::FLOAT:
    return std::to_string(this->GetValueUnsafe<float>());
  case PhysicalType::DOUBLE:
    return std::to_string(this->GetValueUnsafe<double>());
  default:
    std::cout << "cannot convert " << physicalTypeToString(pt) << " to string"
              << std::endl;
  }
}

//===--------------------------------------------------------------------===//
// Type-specific getters
//===--------------------------------------------------------------------===//
bool BooleanValue::Get(const Value &value) {
  return value.GetValueUnsafe<bool>();
}

int8_t TinyIntValue::Get(const Value &value) {
  return value.GetValueUnsafe<int8_t>();
}

int16_t SmallIntValue::Get(const Value &value) {
  return value.GetValueUnsafe<int16_t>();
}

int32_t IntegerValue::Get(const Value &value) {
  return value.GetValueUnsafe<int32_t>();
}

int64_t BigIntValue::Get(const Value &value) {
  return value.GetValueUnsafe<int64_t>();
}

uint8_t UTinyIntValue::Get(const Value &value) {
  return value.GetValueUnsafe<uint8_t>();
}

uint16_t USmallIntValue::Get(const Value &value) {
  return value.GetValueUnsafe<uint16_t>();
}

uint32_t UIntegerValue::Get(const Value &value) {
  return value.GetValueUnsafe<uint32_t>();
}

uint64_t UBigIntValue::Get(const Value &value) {
  return value.GetValueUnsafe<uint64_t>();
}

float FloatValue::Get(const Value &value) {
  return value.GetValueUnsafe<float>();
}

double DoubleValue::Get(const Value &value) {
  return value.GetValueUnsafe<double>();
}

const std::string &StringValue::Get(const Value &value) {
  if (value.is_null) {
    std::cout << "Calling StringValue::Get on a NULL value" << std::endl;
  }
  //   D_ASSERT(value.type().InternalType() == PhysicalType::VARCHAR);
  //   D_ASSERT(value.value_info_);
  return value.value_info_->Get<StringValueInfo>().GetString();
}

const std::vector<Value> &StructValue::GetChildren(const Value &value) {
  if (value.is_null) {
    std::cout << "Calling StructValue::GetChildren on a NULL value"
              << std::endl;
  }
  //   D_ASSERT(value.type().InternalType() == PhysicalType::STRUCT);
  //   D_ASSERT(value.value_info_);
  return value.value_info_->Get<NestedValueInfo>().GetValues();
}

const std::vector<Value> &ListValue::GetChildren(const Value &value) {
  if (value.is_null) {
    std::cout << "Calling ListValue::GetChildren on a NULL value" << std::endl;
  }
  //   D_ASSERT(value.type().InternalType() == PhysicalType::LIST);
  //   D_ASSERT(value.value_info_);
  return value.value_info_->Get<NestedValueInfo>().GetValues();
}

const Value &UnionValue::GetValue(const Value &value) {
  //   D_ASSERT(value.type().id() == PhysicalTypeId::UNION);
  auto &children = StructValue::GetChildren(value);
  auto tag = children[0].GetValueUnsafe<union_tag_t>();
  //   D_ASSERT(tag < children.size() - 1);
  return children[tag + 1];
}

union_tag_t UnionValue::GetTag(const Value &value) {
  //   D_ASSERT(value.type().id() == PhysicalTypeId::UNION);
  auto children = StructValue::GetChildren(value);
  auto tag = children[0].GetValueUnsafe<union_tag_t>();
  //   D_ASSERT(tag < children.size() - 1);
  return tag;
}

//===--------------------------------------------------------------------===//
// Comparison Operators
//===--------------------------------------------------------------------===//
bool Value::operator==(const Value &rhs) const {
  return ValueOperations::Equals(*this, rhs);
}

bool Value::operator!=(const Value &rhs) const {
  return ValueOperations::NotEquals(*this, rhs);
}

bool Value::operator<(const Value &rhs) const {
  return ValueOperations::LessThan(*this, rhs);
}

bool Value::operator>(const Value &rhs) const {
  return ValueOperations::GreaterThan(*this, rhs);
}

bool Value::operator<=(const Value &rhs) const {
  return ValueOperations::LessThanEquals(*this, rhs);
}

bool Value::operator>=(const Value &rhs) const {
  return ValueOperations::GreaterThanEquals(*this, rhs);
}

bool Value::operator==(const int64_t &rhs) const {
  return *this == Value::Numeric(type_, rhs);
}

bool Value::operator!=(const int64_t &rhs) const {
  return *this != Value::Numeric(type_, rhs);
}

bool Value::operator<(const int64_t &rhs) const {
  return *this < Value::Numeric(type_, rhs);
}

bool Value::operator>(const int64_t &rhs) const {
  return *this > Value::Numeric(type_, rhs);
}

bool Value::operator<=(const int64_t &rhs) const {
  return *this <= Value::Numeric(type_, rhs);
}

bool Value::operator>=(const int64_t &rhs) const {
  return *this >= Value::Numeric(type_, rhs);
}

} // namespace relgo