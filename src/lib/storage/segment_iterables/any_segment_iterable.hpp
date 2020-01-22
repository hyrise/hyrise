#pragma once

#include <iterator>
#include <type_traits>

#include "storage/segment_iterables.hpp"
#include "storage/segment_iterables/any_segment_iterator.hpp"

namespace opossum {

template <typename ValueType>
class AnySegmentIterable;

class BaseSegment;

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

template <typename T>
struct is_any_segment_iterable : std::false_type {};

template <typename T>
struct is_any_segment_iterable<AnySegmentIterable<T>> : std::true_type {};

template <typename IterableT>
constexpr auto is_any_segment_iterable_v = is_any_segment_iterable<IterableT>::value;
/**@}*/

template <typename ValueType>
using AnySegmentIterableFunctorWrapper =
    std::function<void(AnySegmentIterator<ValueType>, AnySegmentIterator<ValueType>)>;

template <typename ValueType>
class BaseAnySegmentIterableWrapper {
 public:
  virtual ~BaseAnySegmentIterableWrapper() = default;
  virtual void with_iterators(const AnySegmentIterableFunctorWrapper<ValueType>& functor_wrapper) const = 0;
  virtual void with_iterators(const std::shared_ptr<const PosList>& position_filter,
                              const AnySegmentIterableFunctorWrapper<ValueType>& functor_wrapper) const = 0;
  virtual size_t size() const = 0;
};

template <typename ValueType, typename IterableT>
class AnySegmentIterableWrapper : public BaseAnySegmentIterableWrapper<ValueType> {
 public:
  explicit AnySegmentIterableWrapper(const IterableT& iterable) : iterable(iterable) {}

  void with_iterators(const AnySegmentIterableFunctorWrapper<ValueType>& functor_wrapper) const override {
    iterable.with_iterators([&](auto begin, const auto end) {
      const auto any_segment_iterator_begin = AnySegmentIterator<ValueType>(begin);
      const auto any_segment_iterator_end = AnySegmentIterator<ValueType>(end);
      functor_wrapper(any_segment_iterator_begin, any_segment_iterator_end);
    });
  }

  void with_iterators(const std::shared_ptr<const PosList>& position_filter,
                      const AnySegmentIterableFunctorWrapper<ValueType>& functor_wrapper) const override {
    if (position_filter) {
      if constexpr (is_point_accessible_segment_iterable_v<IterableT>) {
        iterable.with_iterators(position_filter, [&](auto begin, const auto end) {
          const auto any_segment_iterator_begin = AnySegmentIterator<ValueType>(begin);
          const auto any_segment_iterator_end = AnySegmentIterator<ValueType>(end);
          functor_wrapper(any_segment_iterator_begin, any_segment_iterator_end);
        });
      } else {
        Fail("Point access into non-PointAccessIterable not possible");
      }
    } else {
      with_iterators(functor_wrapper);
    }
  }

  size_t size() const override { return iterable._on_size(); }

  IterableT iterable;
};

/**
 * @brief Makes any segment iterable return type-erased iterators
 *
 * AnySegmentIterable’s sole reason for existence is compile speed.
 * Since iterables are almost always used in highly templated code,
 * the functor or lambda passed to their with_iterators methods is
 * called using many different iterators, which leads to a lot of code
 * being generated.
 *
 * The AnySegmentIterable erases the type of the Iterable and the Iterator, with each value retrieval incurring the cost
 * of two virtual function calls.
 */
template <typename T>
class AnySegmentIterable : public PointAccessibleSegmentIterable<AnySegmentIterable<T>> {
 public:
  using ValueType = T;

  template <typename IterableT>
  explicit AnySegmentIterable(const IterableT& iterable)
      : _iterable_wrapper{std::make_shared<AnySegmentIterableWrapper<T, IterableT>>(iterable)} {
    static_assert(!is_any_segment_iterable_v<IterableT>, "Iterables should not be wrapped twice.");
  }

  AnySegmentIterable(const AnySegmentIterable&) = default;
  AnySegmentIterable(AnySegmentIterable&&) noexcept = default;

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    const auto functor_wrapper = AnySegmentIterableFunctorWrapper<T>{functor};
    _iterable_wrapper->with_iterators(functor_wrapper);
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    const auto functor_wrapper = AnySegmentIterableFunctorWrapper<T>{functor};
    _iterable_wrapper->with_iterators(position_filter, functor_wrapper);
  }

  size_t _on_size() const { return _iterable_wrapper->size(); }

 private:
  std::shared_ptr<BaseAnySegmentIterableWrapper<ValueType>> _iterable_wrapper;
};

template <typename IterableT>
auto erase_type_from_iterable(const IterableT& iterable) {
  // clang-format off
  if constexpr(is_any_segment_iterable_v<IterableT>) {
    return iterable;
  } else {
    return AnySegmentIterable<typename IterableT::ValueType>{iterable};
  }
  // clang-format on
}

template <typename IterableT>
decltype(auto) erase_type_from_iterable_if_debug(const IterableT& iterable) {
#if HYRISE_DEBUG
  return erase_type_from_iterable(iterable);
#else
  return iterable;
#endif
}

namespace detail {

// We want to instantiate create_any_segment_iterable() for all data types, but our EXPLICITLY_INSTANTIATE_DATA_TYPES
// macro only supports classes. So we wrap create_any_segment_iterable() in this class and instantiate the class in the
// .cpp
template <typename T>
class CreateAnySegmentIterable {
 public:
  static AnySegmentIterable<T> create(const BaseSegment& base_segment);
};
}  // namespace detail

template <typename T>
AnySegmentIterable<T> create_any_segment_iterable(const BaseSegment& base_segment) {
  return opossum::detail::CreateAnySegmentIterable<T>::create(base_segment);
}

}  // namespace opossum
