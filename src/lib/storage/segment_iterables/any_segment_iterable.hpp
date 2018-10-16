#pragma once

#include <iterator>
#include <type_traits>

#include "storage/segment_iterables.hpp"
#include "storage/segment_iterables/any_segment_iterator.hpp"

namespace opossum {

template <typename IterableT>
class AnySegmentIterable;

/**
 * @brief Wraps passed segment iterable in an AnySegmentIterable
 *
 * Iterators of returned iterables will all have the same type,
 * which reduces compile times due to fewer template instantiations.
 *
 * Returns iterable if it has already been wrapped
 */
template <typename IterableT>
auto erase_type_from_iterable(const IterableT& iterable);

/**
 * @brief Wraps passed segment iterable in an AnySegmentIterable in debug mode
 */
template <typename IterableT>
decltype(auto) erase_type_from_iterable_if_debug(const IterableT& iterable);

/**
 * @defgroup AnySegmentIterable Traits
 * @{
 */

template <typename IterableT>
struct is_any_segment_iterable : std::false_type {};

template <typename IterableT>
struct is_any_segment_iterable<AnySegmentIterable<IterableT>> : std::true_type {};

template <typename IterableT>
constexpr auto is_any_segment_iterable_v = is_any_segment_iterable<IterableT>::value;
/**@}*/

/**
 * @brief Makes any segment iterable return type-erased iterators
 *
 * AnySegmentIterable’s sole reason for existence is compile speed.
 * Since iterables are almost always used in highly templated code,
 * the functor or lambda passed to their with_iterators methods is
 * called using many different iterators, which leads to a lot of code
 * being generated. This affects compile times. The AnySegmentIterator
 * alleviates the long compile times by erasing the iterators’ types and
 * thus reducing the number of instantiations to one (for each segment type).
 *
 * The iterators forwarded are of type AnySegmentIterator<T>. They wrap
 * any segment iterator with the cost of a virtual function call for each access.
 */
template <typename IterableT>
class AnySegmentIterable : public PointAccessibleSegmentIterable<AnySegmentIterable<IterableT>> {
  static_assert(!is_any_segment_iterable_v<IterableT>, "Iterables should not be wrapped twice.");

 public:
  explicit AnySegmentIterable(const IterableT& iterable) : _iterable{iterable} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    _iterable._on_with_iterators([&functor](auto it, auto end) {
      using SegmentIteratorValueT = typename std::iterator_traits<decltype(it)>::value_type;
      using DataTypeT = typename SegmentIteratorValueT::Type;

      auto any_it = AnySegmentIterator<DataTypeT>{it};
      auto any_end = AnySegmentIterator<DataTypeT>{end};

      functor(any_it, any_end);
    });
  }

  template <typename Functor>
  void _on_with_iterators(const PosList& position_filter, const Functor& functor) const {
    _iterable._on_with_iterators(position_filter, [&functor](auto it, auto end) {
      using SegmentIteratorValueT = typename std::iterator_traits<decltype(it)>::value_type;
      using DataTypeT = typename SegmentIteratorValueT::Type;

      auto any_it = AnySegmentIterator<DataTypeT>{it};
      auto any_end = AnySegmentIterator<DataTypeT>{end};

      functor(any_it, any_end);
    });
  }

  size_t _on_size() const { return _iterable._on_size(); }

 private:
  IterableT _iterable;
};

template <typename IterableT>
auto erase_type_from_iterable(const IterableT& iterable) {
  // clang-format off
  if constexpr(is_any_segment_iterable_v<IterableT>) {
    return iterable;
  } else {
    return AnySegmentIterable{iterable};
  }
  // clang-format on
}

template <typename IterableT>
decltype(auto) erase_type_from_iterable_if_debug(const IterableT& iterable) {
#if IS_DEBUG
  return erase_type_from_iterable(iterable);
#else
  return iterable;
#endif
}

}  // namespace opossum
