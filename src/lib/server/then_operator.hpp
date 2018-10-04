#pragma once

#include <boost/thread/future.hpp>

// This code is inspired by the code examples of user Yakk on StackExchange
// https://codereview.stackexchange.com/q/23179
// https://stackoverflow.com/a/32320053

// It helps us to avoid boilerplate code in three ways:
// 1. It automatically 'get's the incoming future's result,
//    thereby throwing previously occurred exceptions
// 2. It passes the boost::launch::sync launch policy to the continuation builder
//    which avoids additional threads to be spawned to execute the continuations
// 3. It automatically unwraps future<future<*>> objects in case that the continuation
//    returns another future

// Example:
// Given the following methods:
//  - boost::future<std::vector<char>> receive_bytes();
//  - boost::future<void> send_bytes(std::vector<char>);
//
// Usage with plain boost::future<>::then
//   boost::future<void> future =
//     receive_bytes()
//       .then(boost::launch::sync, [] (boost::future<std::vector<char>> result) {
//         auto bytes = result.get(); // Throws exceptions that occurred in receive_bytes
//         return send_bytes(bytes);
//       ).unwrap();  // The lambda returns another future, so without unwrap(),
//                    // we'd have a boost::future<boost::future<void>>
//
// Equivalent using then 'operator':
//   boost::future<void> future = receive_bytes() >> then >> send_bytes;

namespace opossum {
namespace then_operator {

template <class D>
struct make_operator {
  make_operator() {}
};

template <class T, class O>
struct half_apply {
  T&& lhs;
};

template <class Lhs, class Op>
half_apply<Lhs, Op> operator>>(Lhs&& lhs, make_operator<Op>) {
  return {std::forward<Lhs>(lhs)};
}

template <class Lhs, class Op, class Rhs>
auto operator>>(half_apply<Lhs, Op>&& lhs, Rhs&& rhs)
    -> decltype(invoke(std::forward<Lhs>(lhs.lhs), Op{}, std::forward<Rhs>(rhs))) {
  return invoke(std::forward<Lhs>(lhs.lhs), Op{}, std::forward<Rhs>(rhs));
}

struct then_t {};
static const make_operator<then_t> then;

template <typename T>
struct is_future : public std::false_type {};

template <typename T>
struct is_future<boost::future<T>> : public std::true_type {};

template <typename T>
inline constexpr bool is_future_v = is_future<T>::value;

// handle future<void> inputs with lambdas producing future<*> outputs
template <class F, class R = std::result_of_t<std::decay_t<F>()>>
auto invoke(boost::future<void>&& lhs, then_t, F&& f) -> std::enable_if_t<is_future_v<R>, R> {
  return lhs
      .then(boost::launch::sync,
            [f = std::forward<F>(f)](boost::future<void> fut) mutable -> R {
              fut.get();
              return std::move(f)();
            })
      .unwrap();
}

// handle future<T> inputs with lambdas producing future<*> outputs
template <class T, class F, class R = std::result_of_t<std::decay_t<F>(T)>>
auto invoke(boost::future<T>&& lhs, then_t, F&& f) -> std::enable_if_t<is_future_v<R>, R> {
  return lhs
      .then(boost::launch::sync,
            [f = std::forward<F>(f)](boost::future<T> fut) mutable -> R {
              return std::move(f)(std::forward<T>(fut.get()));
            })
      .unwrap();
}

// handle future<void> inputs
template <class F, class R = std::result_of_t<std::decay_t<F>()>>
auto invoke(boost::future<void>&& lhs, then_t, F&& f) -> std::enable_if_t<!is_future_v<R>, boost::future<R>> {
  return lhs.then(boost::launch::sync, [f = std::forward<F>(f)](boost::future<void> fut) mutable -> R {
    fut.get();
    return std::move(f)();
  });
}

// handle future<T> inputs
template <class T, class F, class R = std::result_of_t<std::decay_t<F>(T)>>
auto invoke(boost::future<T>&& lhs, then_t, F&& f) -> std::enable_if_t<!is_future_v<R>, boost::future<R>> {
  return lhs.then(boost::launch::sync, [f = std::forward<F>(f)](boost::future<T> fut) mutable -> R {
    return std::move(f)(std::forward<T>(fut.get()));
  });
}

}  // namespace then_operator
}  // namespace opossum
