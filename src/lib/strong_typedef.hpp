#pragma once

#include <boost/config.hpp>
#include <boost/operators.hpp>
#include <boost/type_traits/has_nothrow_assign.hpp>
#include <boost/type_traits/has_nothrow_constructor.hpp>
#include <boost/type_traits/has_nothrow_copy.hpp>

#include <functional>
#include <iostream>
#include <limits>

/*
 * This is an extension of boost's BOOST_STRONG_TYPEDEF.
 * Changes include:
 *  - static typedef referring to the enclosed type
 *  - constexpr constructor
 *  - std::hash specialization
 */

#define STRONG_TYPEDEF(T, D)                                                                                      \
  namespace opossum {                                                                                             \
  struct D : boost::totally_ordered1<D, boost::totally_ordered2<D, T>> {                                          \
    typedef T base_type;                                                                                          \
    T t;                                                                                                          \
    constexpr explicit D(const T& t_) BOOST_NOEXCEPT_IF(boost::has_nothrow_copy_constructor<T>::value) : t(t_) {} \
    D() BOOST_NOEXCEPT_IF(boost::has_nothrow_default_constructor<T>::value) : t() {}                              \
    D& operator=(const T& other) BOOST_NOEXCEPT_IF(boost::has_nothrow_assign<T>::value) {                         \
      t = other;                                                                                                  \
      return *this;                                                                                               \
    }                                                                                                             \
    operator const T&() const { return t; }                                                                       \
    operator T&() { return t; }                                                                                   \
    bool operator==(const D& other) const { return t == other.t; }                                                \
    bool operator<(const D& other) const { return t < other.t; }                                                  \
  };                                                                                                              \
                                                                                                                  \
  inline std::ostream& operator<<(std::ostream& stream, const D& value) { return stream << value.t; }             \
                                                                                                                  \
  } /* NOLINT */                                                                                                  \
                                                                                                                  \
  namespace std {                                                                                                 \
  template <>                                                                                                     \
  struct hash<::opossum::D> : public unary_function<::opossum::D, size_t> {                                       \
    size_t operator()(const ::opossum::D& x) const { return hash<T>{}(x); }                                       \
  };                                                                                                              \
  template <>                                                                                                     \
                                                                                                                  \
  struct numeric_limits<::opossum::D> {                                                                           \
    static typename std::enable_if_t<std::is_arithmetic_v<T>, ::opossum::D> min() {                               \
      return ::opossum::D(numeric_limits<T>::min());                                                              \
    }                                                                                                             \
    static typename std::enable_if_t<std::is_arithmetic_v<T>, ::opossum::D> max() {                               \
      return ::opossum::D(numeric_limits<T>::max());                                                              \
    }                                                                                                             \
  };                                                                                                              \
  } /* NOLINT */                                                                                                  \
  static_assert(true, "End call of macro with a semicolon")
