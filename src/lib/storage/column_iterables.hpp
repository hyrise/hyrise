#pragma once

#include <array>
#include <optional>
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
   * By default, iterables may call functors passed to with_iterators()
   * several times with iterators to different parts of the column.
   * Call require_single_functor_call() if your code requires a single call.
   */
  void require_single_functor_call() { _single_call_required = true; }
  bool single_functor_call_required() const { return _single_call_required; }

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }

 private:
  bool _single_call_required = false;
};

struct ColumnPointAccessPlan {
  PosListIterator begin;
  PosListIterator end;
  ChunkOffset begin_chunk_offset;
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
  void with_iterators(std::optional<ColumnPointAccessPlan> plan, const Functor& functor) const {
    if (!plan) {
      _self()._on_with_iterators(functor);
    } else {
      _self()._on_with_iterators(*plan, functor);
    }
  }

  using ColumnIterable<Derived>::for_each;  // needed because of “name hiding”

  template <typename Functor>
  void for_each(std::optional<ColumnPointAccessPlan> plan, const Functor& functor) const {
    with_iterators(plan, [&functor](auto it, auto end) {
      for (; it != end; ++it) {
        functor(*it);
      }
    });
  }

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

}  // namespace opossum
