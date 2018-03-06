#pragma once
// This is an adaptation of boost's own implementation boost::asio::use_future,
// which returns a std::future<>. However, because we want to use features not
// yet included in the STL, we need boost::future<>s instead. Suprisingly boost
// does not include an implementation, so we adapted boost's implementation by
// repacing all occurrences of std::future with boost::future and std::promise
// with boost::promise, respectively.
// https://github.com/boostorg/asio/blob/master/include/boost/asio/use_future.hpp

#include <boost/asio/detail/config.hpp>
#include <boost/asio/detail/push_options.hpp>
#include <boost/version.hpp>

#include <memory>

namespace boost {
namespace asio {

/// Class used to specify that an asynchronous operation should return a future.
/**
 * The use_future_t class is used to indicate that an asynchronous operation
 * should return a std::future object. A use_future_t object may be passed as a
 * handler to an asynchronous operation, typically using the special value @c
 * boost::asio::use_future. For example:
 *
 * @code std::future<std::size_t> my_future
 *   = my_socket.async_read_some(my_buffer, boost::asio::use_future); @endcode
 *
 * The initiating function (async_read_some in the above example) returns a
 * future that will receive the result of the operation. If the operation
 * completes with an error_code indicating failure, it is converted into a
 * system_error and passed back to the caller via the future.
 */
template <typename Allocator = std::allocator<void> >
class use_boost_future_t {
 public:
  /// The allocator type. The allocator is used when constructing the
  /// @c std::promise object for a given asynchronous operation.
  typedef Allocator allocator_type;

  /// Construct using default-constructed allocator.
  BOOST_ASIO_CONSTEXPR use_boost_future_t() {}

  /// Construct using specified allocator.
  explicit use_boost_future_t(const Allocator& allocator) : allocator_(allocator) {}

  /// Specify an alternate allocator.
  template <typename OtherAllocator>
  use_boost_future_t<OtherAllocator> operator[](const OtherAllocator& allocator) const {
    return use_boost_future_t<OtherAllocator>(allocator);
  }

  /// Obtain allocator.
  allocator_type get_allocator() const { return allocator_; }

 private:
  Allocator allocator_;
};

/// A special value, similar to std::nothrow.
/**
 * See the documentation for boost::asio::use_future_t for a usage example.
 */
#if defined(BOOST_ASIO_MSVC)
__declspec(selectany) use_boost_future_t<> use_boost_future;
#elif defined(BOOST_ASIO_HAS_CONSTEXPR) || defined(GENERATING_DOCUMENTATION)
constexpr use_boost_future_t<> use_boost_future;
#endif

}  // namespace asio
}  // namespace boost

#include <boost/asio/detail/pop_options.hpp>

// TODO(anyone): The implementation version derived from boost 1.64 does not work with
// older boost versions (and vice versa), so we have to maintain two implementations
// until newer boost versions are available through standard package repositories
#if BOOST_VERSION >= 106400
#include "use_boost_future_impl.hpp"
#else
#include "use_boost_future_legacy_impl.hpp"
#endif
