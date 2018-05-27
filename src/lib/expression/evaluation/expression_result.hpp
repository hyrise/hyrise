#pragma once

#include <vector>

#include "boost/variant.hpp"
#include "boost/variant/apply_visitor.hpp"

#include "storage/column_iterables/column_iterator_values.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "null_value.hpp"
#include "utils/assert.hpp"

namespace opossum {

// clang-format off
template<typename T> using NullableValue      = std::optional<T>;
template<typename T> using NullableValues     = std::pair<std::vector<T>, std::vector<bool>>;
template<typename T> using NonNullableValues  = std::vector<T>;

template<typename T> using ExpressionResult = boost::variant<
  // Don't change the order! is_*() functions rely on which()
  NullableValue<T>,
  NullableValues<T>,
  NonNullableValues<T>
>;

template<typename T> bool is_nullable_value(const ExpressionResult<T>& result)      { return result.which() == 0; }
template<typename T> bool is_nullable_values(const ExpressionResult<T>& result)     { return result.which() == 1; }
template<typename T> bool is_non_nullable_values(const ExpressionResult<T>& result) { return result.which() == 2; }

template<typename T> struct expression_result_data_type                       { };
template<>           struct expression_result_data_type<NullValue>            { using type = NullValue; };
template<typename T> struct expression_result_data_type<NullableValue<T>>     { using type = T; };
template<typename T> struct expression_result_data_type<NullableValues<T>>    { using type = T; };
template<typename T> struct expression_result_data_type<NonNullableValues<T>> { using type = T; };

// Default is true, so that all ColumnIterators are though of as Series
template<typename T> struct is_series                       { static constexpr bool value = true; };
template<>           struct is_series<NullValue>            { static constexpr bool value = false; };
template<typename T> struct is_series<NullableValue<T>>     { static constexpr bool value = false; };

template<typename T> constexpr bool is_null_v = std::is_same_v<T, NullValue>;

template<typename T> struct is_nullable_value_t                      { static constexpr bool value = false; };
template<typename T> struct is_nullable_value_t<NullableValue<T>>    { static constexpr bool value = true; };
template<typename T> constexpr bool is_nullable_value_v = is_nullable_value_t<T>::value;

template<typename T> struct is_nullable_values_t                      { static constexpr bool value = false; };
template<typename T> struct is_nullable_values_t<NullableValues<T>>   { static constexpr bool value = true; };
template<typename T> constexpr bool is_nullable_values_v = is_nullable_values_t<T>::value;

template<typename T> struct is_non_nullable_values_t                        { static constexpr bool value = false; };
template<typename T> struct is_non_nullable_values_t<NonNullableValues<T>>  { static constexpr bool value = true; };
template<typename T> constexpr bool is_non_nullable_values_v = is_non_nullable_values_t<T>::value;


/**
 * @defgroup Iterative access for the different ExpressionResult members
 * @{
 */
template<typename T, typename S> void set_expression_result(NullableValue<T>& nullable_value, const ChunkOffset chunk_offset, const S& value, const bool null) {
  if (null) {
    nullable_value.reset();
  } else {
    nullable_value.emplace(static_cast<T>(value));
  }
}

template<typename T, typename S> void set_expression_result(NullableValues<T>& nullable_values, const ChunkOffset chunk_offset, const S& value, const bool null) {
  nullable_values.first[chunk_offset] = static_cast<T>(value);
  nullable_values.second[chunk_offset] = null;
}

template<typename T, typename S> void set_expression_result(NonNullableValues<T>& non_nullable_values, const ChunkOffset chunk_offset, const S& value, const bool null) {
  non_nullable_values[chunk_offset] = static_cast<T>(value);
}
/**
 * @}
 */

template<typename T>
void expression_result_allocate(const T& expression_result, const size_t size) {}

template<typename T>
void expression_result_allocate(NullableValues<T>& nullable_values, const size_t size) {
  nullable_values.first.resize(size);
  nullable_values.second.resize(size);
}

template<typename T>
void expression_result_allocate(NonNullableValues<T>& non_nullable_values, const size_t size) {
  non_nullable_values.resize(size);
}
// clang-format on



}  // namespace opossum