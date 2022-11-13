#pragma once

#include <functional>
#include <iostream>
#include <limits>

#include <boost/config.hpp>
#include <boost/operators.hpp>
#include <boost/type_traits/has_nothrow_assign.hpp>
#include <boost/type_traits/has_nothrow_constructor.hpp>
#include <boost/type_traits/has_nothrow_copy.hpp>

/*
 * This is an extension of boost's BOOST_STRONG_TYPEDEF.
 * Changes include:
 *  - static typedef referring to the enclosed type
 *  - constexpr constructor
 *  - std::hash specialization
 */

#define STRONG_TYPEDEF(T, D)                                                                                      \
  namespace hyrise {                                                                                              \
  struct D : boost::totally_ordered1<D> {                                                                         \
    typedef T base_type;                                                                                          \
    T t;                                                                                                          \
    constexpr explicit D(const T& t_) BOOST_NOEXCEPT_IF(boost::has_nothrow_copy_constructor<T>::value) : t(t_) {} \
    D() BOOST_NOEXCEPT_IF(boost::has_nothrow_default_constructor<T>::value) : t() {}                              \
    D& operator=(const T& other) BOOST_NOEXCEPT_IF(boost::has_nothrow_assign<T>::value) {                         \
      t = other;                                                                                                  \
      return *this;                                                                                               \
    }                                                                                                             \
    operator const T&() const {                                                                                   \
      return t;                                                                                                   \
    }                                                                                                             \
    operator T&() {                                                                                               \
      return t;                                                                                                   \
    }                                                                                                             \
    bool operator==(const D& other) const {                                                                       \
      return t == other.t;                                                                                        \
    }                                                                                                             \
    bool operator<(const D& other) const {                                                                        \
      return t < other.t;                                                                                         \
    }                                                                                                             \
  };                                                                                                              \
                                                                                                                  \
  inline std::ostream& operator<<(std::ostream& stream, const D& value) {                                         \
    return stream << value.t;                                                                                     \
  }                                                                                                               \
                                                                                                                  \
  } /* NOLINT */                                                                                                  \
                                                                                                                  \
  namespace std {                                                                                                 \
  template <>                                                                                                     \
  struct hash<::hyrise::D> {                                                                                      \
    size_t operator()(const ::hyrise::D& x) const {                                                               \
      return hash<T>{}(x);                                                                                        \
    }                                                                                                             \
  };                                                                                                              \
  template <>                                                                                                     \
                                                                                                                  \
  struct numeric_limits<::hyrise::D> {                                                                            \
    static typename std::enable_if_t<std::is_arithmetic_v<T>, ::hyrise::D> min() {                                \
      return ::hyrise::D(numeric_limits<T>::min());                                                               \
    }                                                                                                             \
    static typename std::enable_if_t<std::is_arithmetic_v<T>, ::hyrise::D> max() {                                \
      return ::hyrise::D(numeric_limits<T>::max());                                                               \
    }                                                                                                             \
  };                                                                                                              \
  } /* NOLINT */                                                                                                  \
  namespace hyrise {                                                                                              \
  inline std::size_t hash_value(const D& d) {                                                                     \
    return std::hash<D>()(d);                                                                                     \
  }                                                                                                               \
  } /* NOLINT */                                                                                                  \
  static_assert(true, "End call of macro with a semicolon")
