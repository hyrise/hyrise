#pragma once

#if !defined(BOOST_NO_IOSTREAM)
#include <iosfwd>  // for std::basic_ostream forward declare

#include <boost/detail/templated_streams.hpp>
#endif  // BOOST_NO_IOSTREAM

#include <boost/container_hash/hash.hpp>
#include <boost/mpl/bool.hpp>
#include <boost/type_traits/is_empty.hpp>
#include <boost/type_traits/is_pod.hpp>
#include <boost/type_traits/is_stateless.hpp>

namespace hyrise {

/**
 * @brief Represents SQL null value in AllTypeVariant
 *
 * Based on boost/blank.hpp with changed relational operators
 */
struct NullValue {};

// Relational operators
inline bool operator==(const NullValue& /*lhs*/, const NullValue& /*rhs*/) {
  return false;
}

inline bool operator!=(const NullValue& /*lhs*/, const NullValue& /*rhs*/) {
  return false;
}

inline bool operator<(const NullValue& /*lhs*/, const NullValue& /*rhs*/) {
  return false;
}

inline bool operator<=(const NullValue& /*lhs*/, const NullValue& /*rhs*/) {
  return false;
}

inline bool operator>(const NullValue& /*lhs*/, const NullValue& /*rhs*/) {
  return false;
}

inline bool operator>=(const NullValue& /*lhs*/, const NullValue& /*rhs*/) {
  return false;
}

inline NullValue operator-(const NullValue& /*value*/) {
  return NullValue{};
}

inline size_t hash_value(const NullValue& /*value*/) {
  // Aggregate wants all NULLs in one bucket
  return 0;
}

// Streaming support

#if !defined(BOOST_NO_IOSTREAM)

BOOST_TEMPLATED_STREAM_TEMPLATE(E, T)

inline BOOST_TEMPLATED_STREAM(ostream, E, T) & operator<<(BOOST_TEMPLATED_STREAM(ostream, E, T) & out,
                                                          const NullValue&) {
  out << "NULL";
  return out;
}

#endif  // BOOST_NO_IOSTREAM

}  // namespace hyrise

namespace boost {
// Type traits specializations

template <>
struct is_pod<hyrise::NullValue> : mpl::true_ {};

template <>
struct is_empty<hyrise::NullValue> : mpl::true_ {};

template <>
struct is_stateless<hyrise::NullValue> : mpl::true_ {};

}  // namespace boost
