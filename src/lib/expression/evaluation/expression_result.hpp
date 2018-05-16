#pragma once

#include <vector>

#include "boost/variant.hpp"
#include "boost/variant/apply_visitor.hpp"

#include "null_value.hpp"

namespace opossum {

template<typename T> using NullableValues = std::pair<std::vector<T>, std::vector<bool>>;
template<typename T> using NonNullableValues = std::vector<T>;
template<typename T> using NullableArrays = std::vector<NullableValues<T>>;
template<typename T> using NonNullableArrays = std::vector<NonNullableValues<T>>;

// Don't change the order! is_*() functions rely on which()
template<typename T> using ExpressionResult = boost::variant<
NullableValues<T>,
NonNullableValues<T>,
T,
NullValue,
NullableArrays<T>,
NonNullableArrays<T>
>;

template<typename T> bool is_nullable_values(const ExpressionResult<T>& result){ return result.which() == 0; }
template<typename T> bool is_non_nullable_values(const ExpressionResult<T>& result){ return result.which() == 1; }
template<typename T> bool is_value(const ExpressionResult<T>& result) { return result.which() == 2; }
template<typename T> bool is_null(const ExpressionResult<T>& result){ return result.which() == 3; }
template<typename T> bool is_nullable_array(const ExpressionResult<T>& result) { return result.which() == 4; }
template<typename T> bool is_non_nullable_array(const ExpressionResult<T>& result){ return result.which() == 5; }

}  // namespace opossum