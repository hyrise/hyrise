#pragma once

#include <boost/hana/hash.hpp>
#include <boost/hana/value.hpp>

#include <type_traits>

namespace opossum {

/**
 * @defgroup Definition of Enum Constants
 *
 * An enum constant extends the concept of an integral constant to enum types.
 * This makes it possible to use enums in hana data structures such as maps.
 *
 * @see https://github.com/hyrise/hyrise/wiki/enum-constants
 */

template <typename EnumType>
struct enum_constant_tag {
  static_assert(std::is_enum_v<EnumType>, "EnumType must be an enum (class).");

  using value_type = EnumType;
};

template <typename EnumType, EnumType enum_value>
struct enum_constant {
  static_assert(std::is_enum_v<EnumType>, "EnumType must be an enum (class).");

  using hana_tag = enum_constant_tag<EnumType>;

  static constexpr auto value = enum_value;

  constexpr EnumType operator()() const { return value; }
};

/**
 * This templated constant can be used to conveniently
 * instantiate enum constants of any enum (class) type.
 *
 * Example: enum_c<DataType::Int> (compare to: hana::type_c<int32_t>)
 */
// Unfortunately there is a bug in GCC 7.2.0 so we cannot use this yet.
// template <auto enum_value>
// [[maybe_unused]] constexpr auto enum_c = enum_constant<decltype(enum_value), enum_value>{};

// Workaround: enum_c<DataType, DataType::Int> (compare to: hana::integral_c<int, 13>)
template <typename EnumType, EnumType enum_value>
[[maybe_unused]] constexpr auto enum_c = enum_constant<EnumType, enum_value>{};

/**
 * Definition of our own hana concept “is_enum_constant”
 */
template <typename T>
struct is_enum_constant : std::false_type {};

template <typename EnumType>
struct is_enum_constant<enum_constant_tag<EnumType>> : std::true_type {};

template <typename T>
inline constexpr bool is_enum_constant_v = is_enum_constant<T>::value;

/**@}*/

}  // namespace opossum

namespace boost::hana {

/**
 * Implementation of hana::value in order to meet requirements for concept “Constant”
 */
template <typename E>
struct value_impl<E, when<opossum::is_enum_constant_v<E>>> {
  template <typename C>
  static constexpr auto apply() {
    return C::value;
  }
};

/**
 * Implementation of hana::hash in order to meet requirements for concept “Hashable”
 */
template <typename E>
struct hash_impl<E, when<opossum::is_enum_constant_v<E>>> {
  template <typename X>
  static constexpr auto apply(const X& x) {
    return type_c<opossum::enum_constant<decltype(X::value), X::value>>;
  }
};

}  // namespace boost::hana
