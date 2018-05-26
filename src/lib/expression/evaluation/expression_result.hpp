#pragma once

#include <vector>

#include "boost/variant.hpp"
#include "boost/variant/apply_visitor.hpp"

#include "null_value.hpp"
#include "utils/assert.hpp"

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

template<typename T> struct expression_result_data_type { using type = T; };
template<typename T> struct expression_result_data_type<NullableValues<T>> { using type = T; };
template<typename T> struct expression_result_data_type<NonNullableValues<T>> { using type = T; };
template<typename T> struct expression_result_data_type<NullableArrays<T>> { using type = T; };
template<typename T> struct expression_result_data_type<NonNullableArrays<T>> { using type = T; };

template<typename T> struct is_series { static constexpr bool value = false; };
template<typename T> struct is_series<NullableValues<T>> { static constexpr bool value = true; };
template<typename T> struct is_series<NonNullableValues<T>> { static constexpr bool value = true; };
template<typename T> struct is_series<NullableArrays<T>> { static constexpr bool value = true; };
template<typename T> struct is_series<NonNullableArrays<T>> { static constexpr bool value = true; };

template<typename T> struct is_null_t { static constexpr bool value = std::is_same_v<NullValue, T>; };
template<typename T> constexpr bool is_null_v = is_null_t<T>::value;

template<typename T> struct is_value_t { static constexpr bool value = std::is_same_v<expression_result_data_type<T>::type, T>; };
template<typename T> constexpr bool is_value_v = is_value_t<T>::value;

template<typename T> struct is_nullable_values_t { static constexpr bool value = false; };
template<typename T> struct is_nullable_values_t<NullableValues<T>> { static constexpr bool value = true; };
template<typename T> constexpr bool is_nullable_values_v = is_nullable_values_t<T>::value;

template<typename T> struct is_non_nullable_values_t { static constexpr bool value = false; };
template<typename T> struct is_non_nullable_values_t<NonNullableValues<T>> { static constexpr bool value = true; };
template<typename T> constexpr bool is_non_nullable_values_v = is_non_nullable_values_t<T>::value;

/**
 * @defgroup Determine the ExpressionResult member
 * @{
 */
template<typename R, typename A, typename B, typename Functor, typename Enable = void> struct resolve_result_type { using type = NullableValues<R>; };

//template<typename R, typename A, typename B, typename Functor> struct resolve_result_type<R, A, B, Functor,
//  std::enable_if_t<(is_null_v<A> && is_null_v<B>) || ((is_null_v<A> || is_null_v<B>) && Functor::may_produce_value_from_null)>>{ using type = NullValue; };
//
//template<typename R, typename A, typename B> struct resolve_result_type<R, A, B, Functor,
//  std::enable_if_t<(is_value_v<A> && is_value_v<B>)>{ using type = R; };
/**
 * @}
 */

/**
 * @defgroup Iterative access for the different ExpressionResult members
 * @{
 */
template<typename T> const T& get_value(const T& value, const ChunkOffset chunk_offset) { return value; }
template<typename T> bool get_null(const T& value, const ChunkOffset chunk_offset) { return false; }
template<typename T> void set_value(T& out_value, const ChunkOffset chunk_offset, const T& value) { out_value = value; }
template<typename T> void set_null(T& out_value, const ChunkOffset chunk_offset, const bool null) { }

template<typename T> const T& get_value(const NullableValues<T>& nullable_values, const ChunkOffset chunk_offset) { return nullable_values.first[chunk_offset]; }
template<typename T> bool get_null(const NullableValues<T>& nullable_values, const ChunkOffset chunk_offset) { return nullable_values.second[chunk_offset]; }
template<typename T> void set_value(NullableValues<T>& nullable_values, const ChunkOffset chunk_offset, const T& value) { nullable_values.first[chunk_offset] = value; }
template<typename T> void set_null(NullableValues<T>& nullable_values, const ChunkOffset chunk_offset, const bool null) { nullable_values.second[chunk_offset] = null; }

template<typename T> const T& get_value(const NonNullableValues<T>& non_nullable_values, const ChunkOffset chunk_offset) { return non_nullable_values[chunk_offset]; }
template<typename T> bool get_null(const NonNullableValues<T>& non_nullable_values, const ChunkOffset chunk_offset) { return false; }
template<typename T> void set_value(NonNullableValues<T>& non_nullable_values, const ChunkOffset chunk_offset, const T& value) { non_nullable_values[chunk_offset] = value; }
template<typename T> void set_null(NonNullableValues<T>& non_nullable_values, const ChunkOffset chunk_offset, const bool null) { }

template<typename T> const T& get_value(const NullableArrays<T>& non_nullable_values, const ChunkOffset chunk_offset) { Fail("Can't access Arrays with this API"); }
template<typename T> bool get_null(const NullableArrays<T>& non_nullable_values, const ChunkOffset chunk_offset) { Fail("Can't access Arrays with this API"); }
template<typename T> void set_value(NullableArrays<T>& non_nullable_values, const ChunkOffset chunk_offset, const T& value) { Fail("Can't access Arrays with this API"); }
template<typename T> void set_null(NullableArrays<T>& non_nullable_values, const ChunkOffset chunk_offset, const bool null) { Fail("Can't access Arrays with this API"); }

template<typename T> const T& get_value(const NonNullableArrays<T>& non_nullable_values, const ChunkOffset chunk_offset) { Fail("Can't access Arrays with this API"); }
template<typename T> bool get_null(const NonNullableArrays<T>& non_nullable_values, const ChunkOffset chunk_offset) { Fail("Can't access Arrays with this API"); }
template<typename T> void set_value(NonNullableArrays<T>& non_nullable_values, const ChunkOffset chunk_offset, const T& value) { Fail("Can't access Arrays with this API"); }
template<typename T> void set_null(NonNullableArrays<T>& non_nullable_values, const ChunkOffset chunk_offset, const bool null) { Fail("Can't access Arrays with this API"); }

template<> inline const NullValue& get_value<NullValue>(const NullValue& value, const ChunkOffset chunk_offset) { return value; }
template<> inline bool get_null<NullValue>(const NullValue& value, const ChunkOffset chunk_offset) { return true; }
template<> inline void set_value<NullValue>(NullValue& out_value, const ChunkOffset chunk_offset, const NullValue& value) { }
template<> inline void set_null<NullValue>(NullValue& out_value, const ChunkOffset chunk_offset, const bool null) { }
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


}  // namespace opossum