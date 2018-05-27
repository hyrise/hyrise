#include "expression_result_iterators.hpp"

#include <iterator>

namespace opossum {

// Default is true, so that all ColumnIterators are though of as Series
template<typename T> struct is_series_iter                               { static constexpr bool value = true; };
template<>           struct is_series_iter<NullValueIterator>            { static constexpr bool value = false; };
template<typename T> struct is_series_iter<NullableValueIterator<T>>     { static constexpr bool value = false; };
template<typename T> constexpr bool is_series_iter_v = is_series_iter<T>::value;

// Default is true, so that all ColumnIterators are though of as Series
template<typename T> constexpr bool is_nullable_iter_v = std::iterator_traits<T>::value_type::Nullable;
template<typename T> constexpr bool is_null_iter_v = std::is_same_v<T, NullValueIterator>;

template<typename T> struct is_nullable_value_iter                            { static constexpr bool value = false; };
template<typename T> struct is_nullable_value_iter<NullableValueIterator<T>>  { static constexpr bool value = true; };
template<typename T> constexpr bool is_nullable_value_iter_v = is_nullable_value_iter<T>::value;


/**
 * @defgroup    Determine the type of the result of an expression
 *
 *              Selects the appropriate member type of the variant ExpressionResult based on operand types, Functor
 *              type and result data type.
 *
 * @tparam A    Iterator of the operand A
 * @tparam B    Iterator of the operand B
 *
 * @{
 */
template<typename R, typename A, typename B, typename Fn, typename Enable = void> struct determine_expression_result_type { };

template<typename R, typename A, typename B, typename Functor> struct determine_expression_result_type<R, A, B, Functor,
  std::enable_if_t<(is_null_iter_v<A> && is_null_iter_v<B>) || ((is_null_iter_v<A> || is_null_iter_v<B>) && !Functor::may_produce_value_from_null)>
>{ using type = NullableValue<R>; };

template<typename R, typename A, typename B, typename Functor> struct determine_expression_result_type<R, A, B, Functor,
  std::enable_if_t<!is_series_iter_v<A> && !is_series_iter_v<B> && (is_nullable_value_iter_v<A> || is_nullable_value_iter_v<B>)>
>{ using type = NullableValue<R>; };

template<typename R, typename A, typename B, typename Functor> struct determine_expression_result_type<R, A, B, Functor,
  std::enable_if_t<(is_series_iter_v<A> || is_series_iter_v<B>) && (is_nullable_iter_v<A> || is_nullable_iter_v<B>)>
>{ using type = NullableValues<R>; };

template<typename R, typename A, typename B, typename Functor> struct determine_expression_result_type<R, A, B, Functor,
  std::enable_if_t<(is_series_iter_v<A> || is_series_iter_v<B>) && (!is_nullable_iter_v<A> && !is_nullable_iter_v<B>)>
>{ using type = NonNullableValues<R>; };

/** @} */

}  // namespace opossum
