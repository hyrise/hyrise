#pragma once

#include <type_traits>

#include "storage/column_iterables/base_column_iterators.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * @brief base class of all column iterables
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
class ColumnIterable {
 public:
  /**
   * @param f is a generic lambda accepting two iterators as parameters
   */
  template <typename Functor>
  void with_iterators(const Functor& f) const {
    _self()._on_with_iterators(f);
  }

  /**
   * @param f is a generic lambda accepting a column value (i.e. use const auto&)
   */
  template <typename Functor>
  void for_each(const Functor& f) const {
    with_iterators([&f](auto it, auto end) {
      for (; it != end; ++it) {
        f(*it);
      }
    });
  }

  /**
   * @defgroup Functions for the materialization of values and nulls.
   * The following implementations may be overridden by derived classes.
   * @{
   */

  /**
   * Materialize all values in this iterable.
   * @param container   Container with the same value_type as the values in the column
   */
  template <typename Container>
  void materialize_values(Container& container) const {
    for_each([&](const auto& value) { container.push_back(value.value()); });
  }

  /**
   * Materialize all values in this iterable as std::optional<ValueType>. std::nullopt if value is NULL.
   * @param container   Container with value_type std::pair<bool, T>, where
   *                        bool indicates whether the value is NULL or not
   *                        T is the same as the type of the values in the column
   *                        pair in favour over optional to avoid branches for initialization
   */
  template <typename Container>
  void materialize_values_and_nulls(Container& container) const {
    for_each([&](const auto& value) { container.push_back(std::make_pair(value.is_null(), value.value())); });
  }

  /**
   * Materialize all null values in this Iterable.
   * @param container   The container with value_type bool storing the information whether a value is NULL or not
   */
  template <typename Container>
  void materialize_nulls(Container& container) const {
    for_each([&](const auto& value) { container.push_back(value.is_null()); });
  }

  /** @} */

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

/**
 * @brief base class of all point-accessible column iterables
 *
 * Extends the interface of ColumnIterable by two variants of
 * with_iterators and for_each. These methods accept in addition
 * to the generic lambda mapped chunk offsets (i.e. a ChunkOffsetList).
 * In most cases, this list will be generated from a pos_list of a
 * reference column (see chunk_offset_mapping.hpp). When such a list is
 * passed, the used iterators only iterate over the chunk offsets that
 * were included in the pos_list; everything else is skipped.
 */
template <typename Derived>
class PointAccessibleColumnIterable : public ColumnIterable<Derived> {
 public:
  using ColumnIterable<Derived>::with_iterators;  // needed because of “name hiding”

  template <typename Functor>
  void with_iterators(const ChunkOffsetsList* mapped_chunk_offsets, const Functor& functor) const {
    if (mapped_chunk_offsets == nullptr) {
      _self()._on_with_iterators(functor);
    } else {
      _self()._on_with_iterators(*mapped_chunk_offsets, functor);
    }
  }

  using ColumnIterable<Derived>::for_each;  // needed because of “name hiding”

  template <typename Functor>
  void for_each(const ChunkOffsetsList* mapped_chunk_offsets, const Functor& functor) const {
    with_iterators(mapped_chunk_offsets, [&functor](auto it, auto end) {
      for (; it != end; ++it) {
        functor(*it);
      }
    });
  }

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

}  // namespace opossum
