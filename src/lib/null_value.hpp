#pragma once

#if !defined(BOOST_NO_IOSTREAM)
#include <iosfwd>  // for std::basic_ostream forward declare
#include "boost/detail/templated_streams.hpp"
#endif  // BOOST_NO_IOSTREAM

#include "boost/functional/hash.hpp"
#include "boost/mpl/bool.hpp"
#include "boost/type_traits/is_empty.hpp"
#include "boost/type_traits/is_pod.hpp"
#include "boost/type_traits/is_stateless.hpp"

namespace opossum {

/**
 * @brief Represents SQL null value in AllTypeVariant
 *
 * Based on boost/blank.hpp with changed relational operators
 */
struct NullValue {};

// Relational operators
inline bool operator==(const NullValue&, const NullValue&) { return false; }
inline bool operator!=(const NullValue&, const NullValue&) { return false; }
inline bool operator<(const NullValue&, const NullValue&) { return false; }
inline bool operator<=(const NullValue&, const NullValue&) { return false; }
inline bool operator>(const NullValue&, const NullValue&) { return false; }
inline bool operator>=(const NullValue&, const NullValue&) { return false; }
inline NullValue operator-(const NullValue&) { return NullValue{}; }

inline size_t hash_value(const NullValue& null_value) {
  // Aggregate wants all NULLs in one bucket
  return 0;
}

// Streaming support

#if !defined(BOOST_NO_IOSTREAM)

BOOST_TEMPLATED_STREAM_TEMPLATE(E, T)
inline BOOST_TEMPLATED_STREAM(ostream, E, T) & operator<<(BOOST_TEMPLATED_STREAM(ostream, E, T) & out,
                                                          const opossum::NullValue&) {
  out << "NULL";
  return out;
}

#endif  // BOOST_NO_IOSTREAM

}  // namespace opossum

namespace boost {
// Type traits specializations

template <>
struct is_pod<opossum::NullValue> : mpl::true_ {};

template <>
struct is_empty<opossum::NullValue> : mpl::true_ {};

template <>
struct is_stateless<opossum::NullValue> : mpl::true_ {};

}  // namespace boost
