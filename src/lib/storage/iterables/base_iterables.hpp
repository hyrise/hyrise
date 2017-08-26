#pragma once

#include "base_iterators.hpp"

#include "utils/assert.hpp"

#include "types.hpp"

namespace opossum {

/**
 * @brief base class of all iterables
 *
 * Implements the two methods get_iterators and get_iterators_no_indices,
 * which both accept a generic lambda (or similar) and call this lambda
 * passing a begin and end iterator to the underlying data structure
 * as parameters. Depending on this data structure, the generic lambda
 * may be instantiated for not one but many sets of iterators. For example,
 * the data structure might be accessed via a list of indices or might be
 * nullable or non-nullable.
 *
 * This results in a large amount of code. In cases where one is certain that
 * no indices (i.e. a ChunkOffsetsList) have been passed, get_iterators_no_indices
 * can be used. This method won’t instantiate the lambda for indexed iterators, hence,
 * reduce the size of the compiled binary file.
 *
 *
 * Example Usage
 *
 * auto iterable = ValueColumnIterable<int>{value_column};
 * iterable.get_iterators([&](auto it, auto end) {
 *   for (; it != end; ++it) {
 *     auto value = *it;
 *
 *     if (value.is_null()) { ... }
 *
 *     consume(value.value());
 *   }
 * });
 *
 */
template <typename Derived>
class BaseIterable {
 public:
  /**
   * @param f is a generic lambda accepting to iterators as parameters
   */
  template <typename Functor>
  void get_iterators(const Functor& f) {
    self()._on_get_iterators(f);
  }

  /**
   * Does the same as get_iterators but is needed for a specialization
   * in BaseIndexableIterable
   *
   * @param f is a generic lambda accepting to iterators as parameters
   */
  template <typename Functor>
  void get_iterators_no_indices(const Functor& f) {
    self()._on_get_iterators(f);
  }

 private:
  const Derived& self() const { return static_cast<const Derived&>(*this); }
};

/**
 * @brief base class of all indexable iterables
 */
template <typename Derived>
class BaseIndexableIterable {
 public:
  explicit BaseIndexableIterable(const ChunkOffsetsList* mapped_chunk_offsets = nullptr)
      : _mapped_chunk_offsets{mapped_chunk_offsets} {}

  template <typename Functor>
  void get_iterators(const Functor& f) const {
    if (_mapped_chunk_offsets == nullptr) {
      self()._on_get_iterators_without_indices(f);
    } else {
      self()._on_get_iterators_with_indices(f);
    }
  }

  template <typename Functor>
  void get_iterators_no_indices(const Functor& f) const {
    DebugAssert(_mapped_chunk_offsets == nullptr, "Mapped chunk offsets must be a nullptr.");

    self()._on_get_iterators_without_indices(f);
  }

 protected:
  const ChunkOffsetsList* _mapped_chunk_offsets;

 private:
  const Derived& self() const { return static_cast<const Derived&>(*this); }
};

}  // namespace opossum
