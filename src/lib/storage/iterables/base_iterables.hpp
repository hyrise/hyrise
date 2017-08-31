#pragma once

#include "base_iterators.hpp"

#include "utils/assert.hpp"

#include "types.hpp"

namespace opossum {

/**
 * @brief base class of all iterables
 *
 * Implements the two methods with_iterators and with_iterators_no_indices,
 * which both accept a generic lambda (or similar) and call this lambda
 * passing a begin and end iterator to the underlying data structure
 * as parameters. Depending on this data structure, the generic lambda
 * may be instantiated for not one but many sets of iterators. For example,
 * the data structure might be accessed via a list of indices or might be
 * nullable or non-nullable. This results in a large amount of code.
 *
 * In cases where one is certain that no list of chunk offset mappings
 * (i.e. a ChunkOffsetsList) have been passed, with_iterators_no_indices
 * can be used. This method won’t instantiate the lambda for indexed
 * iterators, hence, reduce the size of the compiled binary file.
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
 */
template <typename Derived>
class BaseIterable {
 public:
  /**
   * @param f is a generic lambda accepting two iterators as parameters
   */
  template <typename Functor>
  void with_iterators(const Functor& f) {
    _self()._on_with_iterators(f);
  }

  /**
   * Does the same as with_iterators but is needed for a specialization
   * in BaseIndexableIterable
   *
   * @param f is a generic lambda accepting two iterators as parameters
   */
  template <typename Functor>
  void with_iterators_no_indices(const Functor& f) {
    _self()._on_with_iterators(f);
  }

  /**
   * @param f is a generic lambda accepting a reference to column value
   */
  template <typename Functor>
  void for_each(const Functor& f) {
    _self().with_iterators([&f](auto it, auto end) {
      for (; it != end; ++it) {
        auto value = *it;
        f(value);
      }
    });
  }

  /**
   * @param f is a generic lambda accepting a reference to column value
   */
  template <typename Functor>
  void for_each_no_indices(const Functor& f) {
    _self().with_iterators_no_indices([&f](auto it, auto end) {
      for (; it != end; ++it) {
        auto value = *it;
        f(value);
      }
    });
  }

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

/**
 * @brief base class of all indexable iterables
 */
template <typename Derived>
class BaseIndexableIterable : public BaseIterable<Derived> {
 public:
  explicit BaseIndexableIterable(const ChunkOffsetsList* mapped_chunk_offsets = nullptr)
      : _mapped_chunk_offsets{mapped_chunk_offsets} {}

  template <typename Functor>
  void with_iterators(const Functor& f) const {
    if (_mapped_chunk_offsets == nullptr) {
      _self()._on_with_iterators_without_indices(f);
    } else {
      _self()._on_with_iterators_with_indices(f);
    }
  }

  template <typename Functor>
  void with_iterators_no_indices(const Functor& f) const {
    DebugAssert(_mapped_chunk_offsets == nullptr, "Mapped chunk offsets must be a nullptr.");

    _self()._on_with_iterators_without_indices(f);
  }

 protected:
  const ChunkOffsetsList* _mapped_chunk_offsets;

 private:
  const Derived& _self() const { return static_cast<const Derived&>(*this); }
};

}  // namespace opossum
