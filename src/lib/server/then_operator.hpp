#include <boost/thread/future.hpp>

namespace named_operator {

template<class D>struct make_operator{make_operator(){}};

template<class T, char, class O> struct half_apply { T&& lhs; };

template<class Lhs, class Op>
half_apply<Lhs, '>', Op> operator>>( Lhs&& lhs, make_operator<Op> ) {
  return {std::forward<Lhs>(lhs)};
}

template<class Lhs, class Op, class Rhs>
auto operator>>( half_apply<Lhs, '>', Op>&& lhs, Rhs&& rhs )
-> decltype( invoke( std::forward<Lhs>(lhs.lhs), Op{}, std::forward<Rhs>(rhs) ) )
{
  return invoke( std::forward<Lhs>(lhs.lhs), Op{}, std::forward<Rhs>(rhs) );
}

}

namespace opossum {
namespace then_operator {

struct then_t{};
static const named_operator::make_operator<then_t> then;

// handle future<future<T>> inputs
template<class T, class F>
auto invoke( boost::future<boost::future<T>>&& lhs, then_t, F&& f ) -> auto
{
  return lhs.unwrap() >> then >> f;
};

// handle future<void> inputs
template<class F,
  class R = std::result_of_t<std::decay_t<F>()>
>
auto invoke( boost::future<void>&& lhs, then_t, F&& f )
-> boost::future< R >
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
-> boost::future< R >
{
  return lhs.then(boost::launch::sync, [f = std::forward<F>(f)](boost::future<T> fut)mutable->R  {
    return std::move(f)(fut.get());
  });
};
}
}