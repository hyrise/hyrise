#pragma once

#include <type_traits>

#include "resolve_type.hpp"
#include "storage/segment_iterables/abstract_segment_iterators.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * @brief base class of all segment iterables
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
 * that expects a segment value as the parameter (use const auto& as the
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
 * auto iterable = ValueSegmentIterable<int>{value_segment};
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
class SegmentIterable {
 public:
  /**
   * @param functor is a generic lambda accepting two iterators as arguments
   */
  template <typename Functor>
  void with_iterators(const Functor& functor) const {
    _self()._on_with_iterators(functor);
  }

  /**
   * @param functor is a generic lambda accepting a SegmentPosition as an argument
   */
  template <typename Functor>
  void for_each(const Functor& functor) const {
    with_iterators([&functor](auto it, auto end) {
      for (; it != end; ++it) {
        functor(*it);
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
   * @param container   Container with the same value_type as the values in the segment
   */
  template <typename Container>
  void materialize_values(Container& container) const {
    size_t index = container.size();
    container.resize(container.size() + _self()._on_size());
    for_each([&](const auto& value) { container[index++] = value.value(); });
  }

  /**
   * Materialize all values in this iterable as std::optional<ValueType>. std::nullopt if value is NULL.
   * @param container   Container with value_type std::pair<bool, T>, where
   *                        bool indicates whether the value is NULL or not
   *                        T is the same as the type of the values in the segment
   *                        pair in favour over optional to avoid branches for initialization
   */
  template <typename Container>
  void materialize_values_and_nulls(Container& container) const {
    size_t index = container.size();
    container.resize(container.size() + _self()._on_size());
    for_each([&](const auto& value) { container[index++] = std::make_pair(value.is_null(), value.value()); });
  }

  /**
   * Materialize all null values in this Iterable.
   * @param container   The container with value_type bool storing the information whether a value is NULL or not
   */
  template <typename Container>
  void materialize_nulls(Container& container) const {
    size_t index = container.size();
    container.resize(container.size() + _self()._on_size());
    for_each([&](const auto& value) { container[index++] = value.is_null(); });
  }

  /** @} */

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

/**
 * @brief base class of all point-accessible segment iterables
 *
 * Extends the interface of SegmentIterable by two variants of
 * with_iterators and for_each. In addition to the generic lambda,
 * these methods accept a PosList, which is used to filter the results.
 * The list is expected to use only that single chunk. When such a list is
 * passed, the used iterators only iterate over the chunk offsets that
 * were included in the pos_list; everything else is skipped.
 */
template <typename Derived>
class PointAccessibleSegmentIterable : public SegmentIterable<Derived> {
 public:
  using SegmentIterable<Derived>::with_iterators;  // needed because of “name hiding”

  /**
   * @tparam ErasePosListType controls whether AbstractPosLists are erased (i.e., resolved dynamically instead of
   *                          statically), which reduces the compile time at the cost of virtual method calls during
   *                          the run time - see the implementation of resolve_pos_list_type.
   * @param  functor is a generic lambda accepting two iterators as arguments
   */
  template <ErasePosListType erase_pos_list_type = ErasePosListType::OnlyInDebugBuild, typename Functor>
  void with_iterators(const std::shared_ptr<const AbstractPosList>& position_filter, const Functor& functor) const {
    if (!position_filter || dynamic_cast<const EntireChunkPosList*>(&*position_filter)) {
      _self()._on_with_iterators(functor);
    } else {
      DebugAssert(position_filter->references_single_chunk(), "Expected PosList to reference single chunk");

      if constexpr (HYRISE_DEBUG || erase_pos_list_type == ErasePosListType::Always) {
        _self()._on_with_iterators(position_filter, functor);
      } else {
        resolve_pos_list_type<erase_pos_list_type>(position_filter, [&functor, this](auto& resolved_position_filter) {
          _self()._on_with_iterators(resolved_position_filter, functor);
        });
      }
    }
  }

  using SegmentIterable<Derived>::for_each;  // needed because of “name hiding”

  /**
   * @tparam ErasePosListType controls whether AbstractPosLists are erased (i.e., resolved dynamically instead of
   *                          statically), which reduces the compile time at the cost of virtual method calls during
   *                          the run time - see the implementation of resolve_pos_list_type.
   * @param  functor is a generic lambda accepting a SegmentPosition as an argument
   */
  template <ErasePosListType erase_pos_list_type = ErasePosListType::OnlyInDebugBuild, typename Functor>
  void for_each(const std::shared_ptr<const AbstractPosList>& position_filter, const Functor& functor) const {
    DebugAssert(!position_filter || position_filter->references_single_chunk(),
                "Expected PosList to reference single chunk");
    with_iterators<erase_pos_list_type>(position_filter, [&functor](auto it, auto end) {
      for (; it != end; ++it) {
        functor(*it);
      }
    });
  }

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

template <typename T>
constexpr auto is_point_accessible_segment_iterable_v =
    std::is_base_of_v<PointAccessibleSegmentIterable<std::decay_t<T>>, T>;

}  // namespace opossum
