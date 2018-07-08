#pragma once

#include <vector>

#include "boost/variant.hpp"
#include "boost/variant/apply_visitor.hpp"

#include "null_value.hpp"
#include "storage/column_iterables/column_iterator_values.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "utils/assert.hpp"

namespace opossum {


class BaseExpressionResult {
 public:
  virtual ~BaseExpressionResult() = default;
};

/**
 * The typed result of an Expression.
 * Wraps a vector of `values` and a vector of `nulls` that are filled differently, depending on the ExpressionResult
 * being a Series (think: a Column; a value for each row), a Literal, nullable or non-nullable.
 *
 * values
 *      Contains a value for each row if the result is a Series
 *      Contains a single value if the result is a Literal
 *
 * nulls
 *      Is empty if the ExpressionResult is non-nullable
 *      Contains a bool for each element of `values` if the ExpressionResult is nullable
 *
 * Examples:
 *      {values: [1, 2, 3, 4]; nulls: []} --> Series [1, 2, 3, 4]
 *      {values: [1, 2, 3, 4]; nulls: [true, false, true, false]} --> Series [NULL, 2, NULL, 4]
 *      {values: [1]; nulls: []} --> Literal [1]
 *      {values: [1]; nulls: [true]} --> Literal [NULL]
 */
template <typename T>
class ExpressionResult : public BaseExpressionResult {
 public:
  using Type = T;

  static std::shared_ptr<ExpressionResult<T>> make_null() {
    ExpressionResult<T> null_value({{T{}}}, {true});
    return std::make_shared<ExpressionResult<T>>(null_value);
  }

  ExpressionResult() = default;

  ExpressionResult(std::vector<T> values, std::vector<bool> nulls = {false})
  : values(std::move(values)), nulls(std::move(nulls)) {}

  bool is_nullable_series() const { return size() != 1; }
  bool is_literal() const { return size() == 1; }
  bool is_nullable() const { return nulls.size() > 1 || nulls.front(); }

  const T& value(const size_t idx) const {
    DebugAssert(values.size() == 1 || idx < values.size(), "Invalid ExpressionResult access");
    return values[std::min(idx, values.size() - 1)];
  }

  bool null(const size_t idx) const {
    DebugAssert(nulls.size() == 1 || idx < nulls.size(), "Invalid ExpressionResult access");
    return nulls[std::min(idx, nulls.size() - 1)];
  }

  /**
   * Resolve ExpressionResult<T> to ExpressionResultNullableSeries<T>, ExpressionResultNonNullSeries<T> or
   * ExpressionResultLiteral<T>
   *
   * Once resolved, a View doesn't need to do bounds checking when queried for value() or null(), thus reducing overhead
   */
  template <typename Functor>
  void as_view(const Functor& fn) const {
    if (size() == 1 || (nulls.size() == 1 && nulls.front())) {
      fn(ExpressionResultLiteral(values.front(), nulls.front()));
    } else if (nulls.size() == 1 && !nulls.front()) {
      fn(ExpressionResultNonNullSeries(values));
    } else {
      fn(ExpressionResultNullableSeries(values, nulls));
    }
  }

  size_t size() const { return values.size(); }

  std::vector<T> values;
  std::vector<bool> nulls;
};

/**
 * View that looks at an ExpressionResult knowing that is a series and may contains nulls
 */
template <typename T>
class ExpressionResultNullableSeries {
 public:
  using Type = T;

  ExpressionResultNullableSeries(const std::vector<T>& values, const std::vector<bool>& nulls)
      : _values(values), _nulls(nulls) {}

  bool is_series() const { return true; }
  bool is_literal() const { return false; }
  bool is_nullable() const { return !_nulls.empty(); }

  const T& value(const size_t idx) const {
    DebugAssert(idx < _values.size(), "Index out of range");
    return _values[idx];
  }

  size_t size() const { return _values.size(); }

  bool null(const size_t idx) const {
    DebugAssert(idx < _values.size(), "Index out of range");
    return _nulls[idx];
  }

 private:
  const std::vector<T>& _values;
  const std::vector<bool>& _nulls;
};

/**
 * View that looks at an ExpressionResult knowing that is a series, but may not return nulls, so null() always returns
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

  bool null(const size_t idx) const { return false; }

 private:
  const std::vector<T>& _values;
};

/**
 * View that looks at an ExpressionResult knowing that is a literal, so always returns the first element in value() and
 * null(), no matter which index is requested
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
  bool null(const size_t = 0) const { return _null; }

 private:
  T _value;
  bool _null;
};

}  // namespace opossum