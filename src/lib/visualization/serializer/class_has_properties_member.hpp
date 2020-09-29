#pragma once

#include <type_traits>

// see https://stackoverflow.com/questions/1005476/how-to-detect-whether-there-is-a-specific-member-variable-in-class

namespace opossum {

template <typename T, typename U = int>
struct has_properties : std::false_type {};

/**
 * True if T has attribute properties
 * 
 * Usage: 
 * if constexpr (has_properties<SomeClass>::value) {
 *  // use SomeClass::propteries
 * } else {
 *  // SomeClass does not have propteries member
 * }
 */
template <typename T>
struct has_properties<T, decltype((void)T::properties, 0)> : std::true_type {};



// TODO(CAJan93): move has_type_property to its own file?
template <typename T, typename U = int>
struct has_type_property : std::false_type {};

/**
 * True if T has attribute type
 * 
 * Usage: 
 * if constexpr (has_type_property<SomeClass>::value) {
 *  // use SomeClass::_type
 * } else {
 *  // SomeClass does not have _type member
 * }
 */
template <typename T>
struct has_type_property<T, decltype((void)T::_type, 0)> : std::true_type {};



}  // namespace opossum