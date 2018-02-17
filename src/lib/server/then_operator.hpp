#include <boost/thread/future.hpp>

// This code is inspired by the code examples of user Yakk on StackExchange
// https://codereview.stackexchange.com/questions/23179/named-operators-in-c

// It helps us to avoid boilerplate code in three ways:
// 1. It automatically 'get's the incoming future's result, 
//    thereby throwing previously occurred exceptions
// 2. It passes the boost::launch::sync launch policy to the continuation builder
//    which avoids additional threads to be spawned to execute the continuations
// 3. It automatically unwraps future<future<*>> objects in case that the continuation
//    returns another future

namespace opossum {
namespace then_operator {

template<class D>struct make_operator{make_operator(){}};

template<class T, char, class O> struct half_apply { T&& lhs; };

template<class Lhs, class Op>
half_apply<Lhs, '*', Op> operator>>( Lhs&& lhs, make_operator<Op> ) {
  return {std::forward<Lhs>(lhs)};
}

template<class Lhs, class Op, class Rhs>
auto operator>>(half_apply<Lhs, '*', Op>&& lhs, Rhs&& rhs)
-> decltype(invoke(std::forward<Lhs>(lhs.lhs), Op{}, std::forward<Rhs>(rhs)))
{
  return invoke(std::forward<Lhs>(lhs.lhs), Op{}, std::forward<Rhs>(rhs));
}

struct then_t{};
static const make_operator<then_t> then;

template<typename T> struct is_future : public std::false_type {};

template<typename T>
struct is_future<boost::future<T>> : public std::true_type {};

// handle future<void> inputs with lambdas producing future<*> outputs
template<class F,
  class R = std::result_of_t<std::decay_t<F>()>
>
auto invoke( boost::future<void>&& lhs, then_t, F&& f )
-> typename std::enable_if<is_future<R>::value, R>::type
{
  return lhs.then(boost::launch::sync, [f = std::forward<F>(f)](boost::future<void> fut)mutable->R  {
    fut.get();
    return std::move(f)();
  }).unwrap();
};

// handle future<T> inputs with lambdas producing future<*> outputs
template<class T, class F,
  class R = std::result_of_t<std::decay_t<F>(T)>
>
auto invoke( boost::future<T>&& lhs, then_t, F&& f )
-> typename std::enable_if<is_future<R>::value, R>::type
{
  return lhs.then(boost::launch::sync, [f = std::forward<F>(f)](boost::future<T> fut)mutable->R  {
    return std::move(f)(fut.get());
  }).unwrap();
};


// handle future<void> inputs
template<class F,
  class R = std::result_of_t<std::decay_t<F>()>
>
auto invoke( boost::future<void>&& lhs, then_t, F&& f )
-> typename std::enable_if<!is_future<R>::value, boost::future<R>>::type
{
  return lhs.then(boost::launch::sync, [f = std::forward<F>(f)](boost::future<void> fut)mutable->R  {
    fut.get();
    return std::move(f)();
  });
};

// handle future<T> inputs
template<class T, class F,
  class R = std::result_of_t<std::decay_t<F>(T)>
>
auto invoke( boost::future<T>&& lhs, then_t, F&& f )
-> typename std::enable_if<!is_future<R>::value, boost::future<R>>::type
{
  return lhs.then(boost::launch::sync, [f = std::forward<F>(f)](boost::future<T> fut)mutable->R  {
    return std::move(f)(fut.get());
  });
};

}
}