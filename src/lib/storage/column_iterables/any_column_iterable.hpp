#pragma once

#include <iterator>
#include <type_traits>

#include "storage/column_iterables.hpp"
#include "storage/column_iterables/any_column_iterator.hpp"

namespace opossum {

template <typename IterableT>
class AnyColumnIterable;

template <typename IterableT>
struct is_any_column_iterable : std::false_type {};

template <typename IterableT>
struct is_any_column_iterable<AnyColumnIterable<IterableT>> : std::true_type {};

template <typename IterableT>
constexpr auto is_any_column_iterable_v = is_any_column_iterable<IterableT>::value;

/**
 * @brief Makes any column iterable return type erased iterators
 *
 * The iterators forwarded are of type AnyColumnIterator<T>.
 */
template <typename IterableT>
class AnyColumnIterable : public PointAccessibleColumnIterable<AnyColumnIterable<IterableT>> {
  static_assert(!is_any_column_iterable_v<IterableT>, "Iterables should not be wrapped twice.");

 public:
  explicit AnyColumnIterable(const IterableT& iterable) : _iterable{iterable} {
    if (_iterable.single_functor_call_required()) this->require_single_functor_call();
  }

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    _iterable._on_with_iterators([&functor](auto it, auto end) {
      using ColumnIteratorValueT = typename std::iterator_traits<decltype(it)>::value_type;
      using DataTypeT = typename ColumnIteratorValueT::Type;

      auto any_it = AnyColumnIterator<DataTypeT>{it};
      auto any_end = AnyColumnIterator<DataTypeT>{end};

      functor(any_it, any_end);
    });
  }

  template <typename Functor>
  void _on_with_iterators(const ColumnPointAccessPlan& plan, const Functor& functor) const {
    _iterable._on_with_iterators(plan, [&functor](auto it, auto end) {
      using ColumnIteratorValueT = typename std::iterator_traits<decltype(it)>::value_type;
      using DataTypeT = typename ColumnIteratorValueT::Type;

      auto any_it = AnyColumnIterator<DataTypeT>{it};
      auto any_end = AnyColumnIterator<DataTypeT>{end};

      functor(any_it, any_end);
    });
  }

  void require_single_functor_call() {
    ColumnIterable<AnyColumnIterable>::require_single_functor_call();
    _iterable.require_single_functor_call();
  }

 private:
  IterableT _iterable;
};

/**
 * @brief Wraps passed column iterable in an AnyColumnIterable
 *
 * Returns iterable if it has already been wrapped
 */
template <typename IterableT>
auto erase_type_from_iterable(const IterableT& iterable) {
  // clang-format off
  if constexpr (is_any_column_iterable_v<IterableT>) {
    return iterable;
  } else {
    return AnyColumnIterable{iterable};
  }
  // clang-format on
}

}  // namespace opossum
