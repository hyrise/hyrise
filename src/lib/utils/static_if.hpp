#pragma once

#include <utility>

namespace opossum {

/**
 * This imitates C++17 constexpr-ifs
 *
 * Example Usage
 *
 * static_if<std::is_floating_point<T>{}>([&](auto f) {
 *   // handle floating point value
 * }).else([&](auto f) {
 *   // handle everything else
 * });
 *
 * Taken from:
 * https://baptiste-wicht.com/posts/2015/07/simulate-static_if-with-c11c14.html
 */

namespace static_if_detail {

struct Identity {
  template <typename T>
  T operator()(T &&x) const {
    return std::forward<T>(x);
  }
};

template <bool Condition>
struct Statement {
  template <typename Functor>
  void then(const Functor &func) {
    func(Identity{});
  }

  template <typename Functor>
  void else_(const Functor &) {}
};

template <>
struct Statement<false> {
  template <typename Functor>
  void then(const Functor &) {}

  template <typename Functor>
  void else_(const Functor &func) {
    func(Identity{});
  }
};

}  // namespace static_if_detail

template <bool Condition, typename Functor>
auto static_if(const Functor &func) {
  static_if_detail::Statement<Condition> if_{};
  if_.then(func);
  return if_;
}

}  // namespace opossum
