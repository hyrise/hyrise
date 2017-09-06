#pragma once

#include "base_iterators.hpp"

#include "utils/assert.hpp"

#include "types.hpp"

namespace opossum {

/**
 * @brief base class of all iterables
 *
 * Implements the method with_iterators, which accepts a generic lambda
 * (or similar) that expects a begin and end iterator to the underlying
 * data structure as parameters. Depending on this data structure, the
 * generic lambda may be instantiated for not one but many sets of iterators.
 * For example, the data structure might be accessed via a list of indices or
 * might be nullable or non-nullable.
 *
 * For convenience, the class also implements the method for_each, which
 * takes care of iterating over the elements and accepts a generic lambda
 * that expects a column value as the parameter (use const auto& as the
 * parameter declaration!).
 *
 *
 * A note on CRTP (curiously recurring template pattern):
 *
 * The iterables use the CRTP, i.e., the different iterable implementations
 * derive from one of the base iterables and pass themselves on as
 * template arguments. The base class can then cast itself into the sub class
 * and access its methods. The pattern is used here to implicitly define
 * the interface of a few templated methods. Templated methods cannot be
 * virtual in C++, so this could not be done with a normal abstract interface.
 * The advantage is that by looking at these two base iterables, it is clear
 * what interface to expect from any iterable and it also makes the otherwise
 * implicit common interface of iterables more explicit.
 *
 *
 * Example Usage
 *
 * auto iterable = ValueColumnIterable<int>{value_column};
 * iterable.with_iterators([&](auto it, auto end) {
 *   for (; it != end; ++it) {
 *     auto value = *it;
 *
 *     if (value.is_null()) { ... }
 *
 *     consume(value.value());
 *   }
 * });
 *
 * iterable.for_each([&](const auto& value) {
 *   if (value.is_null()) { ... }
 *
 *   consume(value.value());
 * });
 *
 */
template <typename Derived>
class Iterable {
 public:
  /**
   * @param f is a generic lambda accepting two iterators as parameters
   */
  template <typename Functor>
  void with_iterators(const Functor& f) {
    _self()._on_with_iterators(f);
  }

  /**
   * @param f is a generic lambda accepting a column value (i.e. use const auto&)
   */
  template <typename Functor>
  void for_each(const Functor& f) {
    with_iterators([&f](auto it, auto end) {
      for (; it != end; ++it) {
        f(*it);
      }
    });
  }

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

/**
 * @brief base class of all indexable iterables
 *
 * Extends the interface of Iterable by two variants of
 * with_iterators and for_each. These methods accept in addition
 * to the generic lambda mapped chunk offsets (i.e. a ChunkOffsetList).
 * In most cases, this list will be generated from a pos_list of a
 * reference column (see chunk_offset_mapping.hpp). When such a list is
 * passed, the used iterators only iterate over the chunk offsets that
 * were included in the pos_list; everything else is skipped.
 */
template <typename Derived>
class IndexableIterable : public Iterable<Derived> {
 public:
  using Iterable<Derived>::with_iterators;  // needed because of “name hiding”

  template <typename Functor>
  void with_iterators(const ChunkOffsetsList* mapped_chunk_offsets, const Functor& f) const {
    if (mapped_chunk_offsets == nullptr) {
      _self()._on_with_iterators(f);
    } else {
      _self()._on_with_iterators(*mapped_chunk_offsets, f);
    }
  }

  using Iterable<Derived>::for_each;  // needed because of “name hiding”

  template <typename Functor>
  void for_each(const ChunkOffsetsList* mapped_chunk_offsets, const Functor& f) {
    with_iterators(mapped_chunk_offsets, [&f](auto it, auto end) {
      for (; it != end; ++it) {
        f(*it);
      }
    });
  }

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

}  // namespace opossum
