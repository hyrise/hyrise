#pragma once

#include <iterator>

#include "storage/column_iterables/any_column_iterator.hpp"
#include "storage/column_iterables.hpp"

namespace opossum {

/**
 * @brief Makes any column iterable return type erased iterators
 *
 * The iterators forwarded are of type AnyIterator<T>.
 */
template <typename Iterable>
class AnyIterable : public PointAccessibleColumnIterable<AnyIterable<Iterable>> {
 public:
  explicit AnyIterable(const Iterable& iterable) : _iterable{iterable} {}

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
  void _on_with_iterators(const ChunkOffsetsList& mapped_chunk_offsets, const Functor& functor) const {
    _iterable._on_with_iterators(mapped_chunk_offsets, [&functor](auto it, auto end) {
      using ColumnIteratorValueT = typename std::iterator_traits<decltype(it)>::value_type;
      using DataTypeT = typename ColumnIteratorValueT::Type;

      auto any_it = AnyColumnIterator<DataTypeT>{it};
      auto any_end = AnyColumnIterator<DataTypeT>{end};

      functor(any_it, any_end);
    });
  }

 private:
  const Iterable& _iterable;
};

template <typename Iterable>
auto erase_type_from_iterable(const Iterable& iterable) {
  return AnyIterable{iterable};
}

}  // namespace opossum
