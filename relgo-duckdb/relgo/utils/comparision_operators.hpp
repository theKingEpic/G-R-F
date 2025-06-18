#ifndef COMPARISION_OPERATORS_H
#define COMPARISION_OPERATORS_H

#include <cstring>

namespace relgo {

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
struct ValEquals {
  template <class T>
  static inline bool Operation(const T &left, const T &right) {
    return left == right;
  }
};
struct ValNotEquals {
  template <class T>
  static inline bool Operation(const T &left, const T &right) {
    return !ValEquals::Operation(left, right);
  }
};

struct ValGreaterThan {
  template <class T>
  static inline bool Operation(const T &left, const T &right) {
    return left > right;
  }
};

struct ValGreaterThanEquals {
  template <class T>
  static inline bool Operation(const T &left, const T &right) {
    return !ValGreaterThan::Operation(right, left);
  }
};

struct ValLessThan {
  template <class T>
  static inline bool Operation(const T &left, const T &right) {
    return ValGreaterThan::Operation(right, left);
  }
};

struct ValLessThanEquals {
  template <class T>
  static inline bool Operation(const T &left, const T &right) {
    return !ValGreaterThan::Operation(left, right);
  }
};

template <> bool ValEquals::Operation(const float &left, const float &right);
template <> bool ValEquals::Operation(const double &left, const double &right);

template <>
bool ValGreaterThan::Operation(const float &left, const float &right);
template <>
bool ValGreaterThan::Operation(const double &left, const double &right);

template <>
bool ValGreaterThanEquals::Operation(const float &left, const float &right);
template <>
bool ValGreaterThanEquals::Operation(const double &left, const double &right);

// Distinct semantics are from Postgres record sorting. NULL = NULL and not-NULL
// < NULL Deferring to the non-distinct operations removes the need for further
// specialisation.
// TODO: To reverse the semantics, swap left_null and right_null for comparisons
struct ValDistinctFrom {
  template <class T>
  static inline bool Operation(const T &left, const T &right, bool left_null,
                               bool right_null) {
    if (left_null || right_null) {
      return left_null != right_null;
    }
    return ValNotEquals::Operation(left, right);
  }
};

struct ValNotDistinctFrom {
  template <class T>
  static inline bool Operation(const T &left, const T &right, bool left_null,
                               bool right_null) {
    return !ValDistinctFrom::Operation(left, right, left_null, right_null);
  }
};

struct ValDistinctGreaterThan {
  template <class T>
  static inline bool Operation(const T &left, const T &right, bool left_null,
                               bool right_null) {
    if (left_null || right_null) {
      return !right_null;
    }
    return ValGreaterThan::Operation(left, right);
  }
};

struct ValDistinctGreaterThanNullsFirst {
  template <class T>
  static inline bool Operation(const T &left, const T &right, bool left_null,
                               bool right_null) {
    return ValDistinctGreaterThan::Operation(left, right, right_null,
                                             left_null);
  }
};

struct ValDistinctGreaterThanEquals {
  template <class T>
  static inline bool Operation(const T &left, const T &right, bool left_null,
                               bool right_null) {
    return !ValDistinctGreaterThan::Operation(right, left, right_null,
                                              left_null);
  }
};

struct ValDistinctLessThan {
  template <class T>
  static inline bool Operation(const T &left, const T &right, bool left_null,
                               bool right_null) {
    return ValDistinctGreaterThan::Operation(right, left, right_null,
                                             left_null);
  }
};

struct ValDistinctLessThanNullsFirst {
  template <class T>
  static inline bool Operation(const T &left, const T &right, bool left_null,
                               bool right_null) {
    return ValDistinctGreaterThan::Operation(right, left, left_null,
                                             right_null);
  }
};

struct ValDistinctLessThanEquals {
  template <class T>
  static inline bool Operation(const T &left, const T &right, bool left_null,
                               bool right_null) {
    return !ValDistinctGreaterThan::Operation(left, right, left_null,
                                              right_null);
  }
};

//===--------------------------------------------------------------------===//
// Specialized Boolean Comparison Operators
//===--------------------------------------------------------------------===//
template <>
inline bool ValGreaterThan::Operation(const bool &left, const bool &right) {
  return !right && left;
}

} // namespace relgo

#endif