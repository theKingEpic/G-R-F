//===--------------------------------------------------------------------===//
// comparison_operators.cpp
// Description: This file contains the implementation of the comparison
// operations == != >= <= > <
//===--------------------------------------------------------------------===//

#include "comparision_operators.hpp"
#include "value.hpp"

namespace relgo {

template <class T> bool EqualsFloat(T left, T right) {
  if (Value::IsNan(left) && Value::IsNan(right)) {
    return true;
  }
  return left == right;
}

template <> bool ValEquals::Operation(const float &left, const float &right) {
  return EqualsFloat<float>(left, right);
}

template <> bool ValEquals::Operation(const double &left, const double &right) {
  return EqualsFloat<double>(left, right);
}

template <class T> bool GreaterThanFloat(T left, T right) {
  // handle nans
  // nan is always bigger than everything else
  bool left_is_nan = Value::IsNan(left);
  bool right_is_nan = Value::IsNan(right);
  // if right is nan, there is no number that is bigger than right
  if (right_is_nan) {
    return false;
  }
  // if left is nan, but right is not, left is always bigger
  if (left_is_nan) {
    return true;
  }
  return left > right;
}

template <>
bool ValGreaterThan::Operation(const float &left, const float &right) {
  return GreaterThanFloat<float>(left, right);
}

template <>
bool ValGreaterThan::Operation(const double &left, const double &right) {
  return GreaterThanFloat<double>(left, right);
}

template <class T> bool GreaterThanEqualsFloat(T left, T right) {
  // handle nans
  // nan is always bigger than everything else
  bool left_is_nan = Value::IsNan(left);
  bool right_is_nan = Value::IsNan(right);
  // if right is nan, there is no bigger number
  // we only return true if left is also nan (in which case the numbers are
  // equal)
  if (right_is_nan) {
    return left_is_nan;
  }
  // if left is nan, but right is not, left is always bigger
  if (left_is_nan) {
    return true;
  }
  return left >= right;
}

template <>
bool ValGreaterThanEquals::Operation(const float &left, const float &right) {
  return GreaterThanEqualsFloat<float>(left, right);
}

template <>
bool ValGreaterThanEquals::Operation(const double &left, const double &right) {
  return GreaterThanEqualsFloat<double>(left, right);
}

} // namespace relgo