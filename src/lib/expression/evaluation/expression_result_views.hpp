#pragma once

#include <vector>
#include "utils/assert.hpp"

namespace opossum {

/**
 * ExpressionResultViews is a Concept used internally in the ExpressionEvaluator to allow the compiler to throw
 * away, e.g., calls to is_null(), if the ExpressionResult is non-nullable and to omit bounds checks in release builds.
 *
 * An ExpressionResult is turned into an ExpressionResultView by calling ExpressionResult::as_view()
 */

/**
 * View that looks at an ExpressionResult knowing that is a series and may contains nulls
 */
template <typename T>
class ExpressionResultNullableSeries {
 public:
  using Type = T;

  ExpressionResultNullableSeries(const std::vector<T>& values, const std::vector<bool>& nulls)
      : _values(values), _nulls(nulls) {
    DebugAssert(values.size() == nulls.size(), "Need as many values as nulls");
  }

  bool is_series() const { return true; }
  bool is_literal() const { return false; }
  bool is_nullable() const { return true; }

  const T& value(const size_t idx) const {
    DebugAssert(idx < _values.size(), "Index out of range");
    return _values[idx];
  }

  size_t size() const { return _values.size(); }

  bool is_null(const size_t idx) const {
    DebugAssert(idx < _nulls.size(), "Index out of range");
    return _nulls[idx];
  }

 private:
  const std::vector<T>& _values;
  const std::vector<bool>& _nulls;
};

/**
 * View that looks at an ExpressionResult knowing that is a series, but may not return nulls, so is_null() always returns
 * false
 */
template <typename T>
class ExpressionResultNonNullSeries {
 public:
  using Type = T;

  explicit ExpressionResultNonNullSeries(const std::vector<T>& values) : _values(values) {}

  bool is_series() const { return true; }
  bool is_literal() const { return false; }
  bool is_nullable() const { return false; }

  size_t size() const { return _values.size(); }

  const T& value(const size_t idx) const {
    DebugAssert(idx < _values.size(), "Index out of range");
    return _values[idx];
  }

  bool is_null(const size_t idx) const { return false; }

 private:
  const std::vector<T>& _values;
};

/**
 * View that looks at an ExpressionResult knowing that is a literal, so always returns the first element in value() and
 * is_null(), no matter which index is requested
 */
template <typename T>
class ExpressionResultLiteral {
 public:
  using Type = T;

  ExpressionResultLiteral(const T& value, const bool null) : _value(value), _null(null) {}

  bool is_series() const { return false; }
  bool is_literal() const { return true; }
  bool is_nullable() const { return _null; }

  size_t size() const { return 1u; }

  const T& value(const size_t = 0) const { return _value; }
  bool is_null(const size_t = 0) const { return _null; }

 private:
  T _value;
  bool _null;
};

}  // namespace opossum
