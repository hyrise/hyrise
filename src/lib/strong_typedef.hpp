#pragma once

#include <boost/config.hpp>
#include <boost/operators.hpp>
#include <boost/type_traits/has_nothrow_assign.hpp>
#include <boost/type_traits/has_nothrow_constructor.hpp>
#include <boost/type_traits/has_nothrow_copy.hpp>

#include <functional>

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
    D(const D& t_) BOOST_NOEXCEPT_IF(boost::has_nothrow_copy_constructor<T>::value) : t(t_.t) {}                  \
    D& operator=(const D& rhs) BOOST_NOEXCEPT_IF(boost::has_nothrow_assign<T>::value) {                           \
      t = rhs.t;                                                                                                  \
      return *this;                                                                                               \
    }                                                                                                             \
    D& operator=(const T& rhs) BOOST_NOEXCEPT_IF(boost::has_nothrow_assign<T>::value) {                           \
      t = rhs;                                                                                                    \
      return *this;                                                                                               \
    }                                                                                                             \
    operator const T&() const { return t; }                                                                       \
    operator T&() { return t; }                                                                                   \
    bool operator==(const D& rhs) const { return t == rhs.t; }                                                    \
    bool operator<(const D& rhs) const { return t < rhs.t; }                                                      \
  };                                                                                                              \
  } /* NOLINT */                                                                                                  \
                                                                                                                  \
  namespace std {                                                                                                 \
  template <>                                                                                                     \
  struct hash<::opossum::D> : public unary_function<::opossum::D, size_t> {                                       \
    size_t operator()(const ::opossum::D& x) const { return hash<T>{}(x); }                                       \
  };                                                                                                              \
  }  // NOLINT
