#pragma once

#include <boost/asio/async_result.hpp>
#include <boost/asio/detail/config.hpp>
#include <boost/asio/detail/memory.hpp>
#include <boost/asio/handler_type.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <boost/thread/future.hpp>

#include <boost/asio/detail/push_options.hpp>

namespace boost {
namespace asio {
namespace detail {

// Completion handler to adapt a promise as a completion handler.
template <typename T>
class boost_promise_handler {
 public:
  // Construct from use_future special value.
  template <typename Alloc>
  explicit boost_promise_handler(use_boost_future_t<Alloc> uf)
      : promise(std::allocate_shared<boost::promise<T> >(BOOST_ASIO_REBIND_ALLOC(Alloc, char)(uf.get_allocator()),
                                                         std::allocator_arg,
                                                         BOOST_ASIO_REBIND_ALLOC(Alloc, char)(uf.get_allocator()))) {}

  void operator()(T t) { promise->set_value(t); }

  void operator()(const boost::system::error_code& ec, T t) {
    if (ec) {
      promise->set_exception(std::make_exception_ptr(boost::system::system_error(ec)));
    } else {
      promise->set_value(t);
    }
  }

  std::shared_ptr<boost::promise<T> > promise;
};

// Completion handler to adapt a void promise as a completion handler.
template <>
class boost_promise_handler<void> {
 public:
  // Construct from use_future special value. Used during rebinding.
  template <typename Alloc>
  explicit boost_promise_handler(use_boost_future_t<Alloc> uf)
      : promise(std::allocate_shared<boost::promise<void> >(BOOST_ASIO_REBIND_ALLOC(Alloc, char)(uf.get_allocator()),
                                                            std::allocator_arg,
                                                            BOOST_ASIO_REBIND_ALLOC(Alloc, char)(uf.get_allocator()))) {
  }

  void operator()() { promise->set_value(); }

  void operator()(const boost::system::error_code& ec) {
    if (ec) {
      promise->set_exception(std::make_exception_ptr(boost::system::system_error(ec)));
    } else {
      promise->set_value();
    }
  }

  std::shared_ptr<boost::promise<void> > promise;
};

// Ensure any exceptions thrown from the handler are propagated back to the
// caller via the future.
template <typename Function, typename T>
void asio_handler_invoke(Function f, boost_promise_handler<T>* h) {
  std::shared_ptr<boost::promise<T> > p(h->promise);
  try {
    f();
  } catch (...) {
    p->set_exception(std::current_exception());
  }
}

}  // namespace detail

#if !defined(GENERATING_DOCUMENTATION)

// Handler traits specialisation for boost_promise_handler.
template <typename T>
class async_result<detail::boost_promise_handler<T> > {
 public:
  // The initiating function will return a future.
  typedef boost::future<T> type;

  // Constructor creates a new promise for the async operation, and obtains the
  // corresponding future.
  explicit async_result(detail::boost_promise_handler<T>& h) { value_ = h.promise->get_future(); }

  // Obtain the future to be returned from the initiating function.
  type get() { return std::move(value_); }

 private:
  type value_;
};

// Handler type specialisation for use_boost_future_t.
template <typename Allocator, typename ReturnType>
struct handler_type<use_boost_future_t<Allocator>, ReturnType()> {
  typedef detail::boost_promise_handler<void> type;
};

// Handler type specialisation for use_boost_future_t.
template <typename Allocator, typename ReturnType, typename Arg1>
struct handler_type<use_boost_future_t<Allocator>, ReturnType(Arg1)> {
  typedef detail::boost_promise_handler<Arg1> type;
};

// Handler type specialisation for use_boost_future_t.
template <typename Allocator, typename ReturnType>
struct handler_type<use_boost_future_t<Allocator>, ReturnType(boost::system::error_code)> {
  typedef detail::boost_promise_handler<void> type;
};

// Handler type specialisation for use_boost_future_t.
template <typename Allocator, typename ReturnType, typename Arg2>
struct handler_type<use_boost_future_t<Allocator>, ReturnType(boost::system::error_code, Arg2)> {
  typedef detail::boost_promise_handler<Arg2> type;
};

#endif  // !defined(GENERATING_DOCUMENTATION)

}  // namespace asio
}  // namespace boost

#include <boost/asio/detail/pop_options.hpp>
