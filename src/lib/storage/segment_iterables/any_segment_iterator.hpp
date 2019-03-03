#pragma once

#include <memory>

#include "storage/segment_iterables/base_segment_iterators.hpp"

namespace opossum {

namespace detail {

/**
 * Emulates a base class for segment iterators with a virtual interface.
 * It duplicates all methods implemented by segment iterators as part of
 * a virtual interface.
 */
template <typename T>
class AnySegmentIteratorWrapperBase {
 public:
  virtual ~AnySegmentIteratorWrapperBase() = default;

  virtual void increment() = 0;
  virtual void advance(std::ptrdiff_t n) = 0;
  virtual bool equal(const AnySegmentIteratorWrapperBase<T>* other) const = 0;
  virtual std::ptrdiff_t distance_to(const AnySegmentIteratorWrapperBase<T>* other) const = 0;
  virtual SegmentPosition<T> dereference() const = 0;

  /**
   * Segment iterators need to be copyable so we need a way
   * to copy the iterator within the wrapper.
   */
  virtual std::unique_ptr<AnySegmentIteratorWrapperBase<T>> clone() const = 0;
};

/**
 * @brief The class where the wrapped iterator’s methods are called.
 *
 * Passes the virtual method call on to the non-virtual methods of the
 * iterator class passed as template argument.
 */
template <typename T, typename Iterator>
class AnySegmentIteratorWrapper : public AnySegmentIteratorWrapperBase<T> {
 public:
  explicit AnySegmentIteratorWrapper(const Iterator& iterator) : _iterator{iterator} {}

  void increment() final { ++_iterator; }

  void advance(std::ptrdiff_t n) final { _iterator += n; }

  /**
   * Although `other` could have a different type, it is practically impossible,
   * since AnySegmentIterator is only used within AnySegmentIterable.
   */
  bool equal(const AnySegmentIteratorWrapperBase<T>* other) const final {
    const auto casted_other = static_cast<const AnySegmentIteratorWrapper<T, Iterator>*>(other);
    return _iterator == casted_other->_iterator;
  }

  std::ptrdiff_t distance_to(const AnySegmentIteratorWrapperBase<T>* other) const final {
    const auto casted_other = static_cast<const AnySegmentIteratorWrapper<T, Iterator>*>(other);
    return casted_other->_iterator - _iterator;
  }

  SegmentPosition<T> dereference() const final {
    const auto value = *_iterator;
    return {value.value(), value.is_null(), value.chunk_offset()};
  }

  std::unique_ptr<AnySegmentIteratorWrapperBase<T>> clone() const final {
    return std::make_unique<AnySegmentIteratorWrapper<T, Iterator>>(_iterator);
  }

 private:
  Iterator _iterator;
};

}  // namespace detail

template <typename T>
class AnySegmentIterable;

/**
 * @brief Erases the type of any segment iterator
 *
 * Erases the type of any segment iterator by wrapping it
 * in a templated class inheriting from a common base class.
 * The base class specifies a virtual interface which is
 * implemented by the templated sub-class.
 *
 * AnySegmentIterator inherits from BaseSegmentIterator and
 * thus has the same interface as all other segment iterators.
 *
 * AnySegmentIterator exists only to improve compile times and should
 * not be used outside of AnySegmentIterable.
 *
 * For another example for type erasure see: https://en.wikibooks.org/wiki/More_C%2B%2B_Idioms/Type_Erasure
 */
template <typename T>
class AnySegmentIterator : public BaseSegmentIterator<AnySegmentIterator<T>, SegmentPosition<T>> {
 public:
  using ValueType = T;
  using IterableType = AnySegmentIterable<T>;

  /**
   * Prevents AnySegmentIterator from being created
   * by anything else but AnySegmentIterable
   *
   * @{
   */
  template <typename U>
  friend class AnySegmentIterable;

  template <typename Iterator>
  explicit AnySegmentIterator(const Iterator& iterator)
      : _wrapper{std::make_unique<opossum::detail::AnySegmentIteratorWrapper<T, Iterator>>(iterator)} {}
  /**@}*/

 public:
  AnySegmentIterator(const AnySegmentIterator& other) : _wrapper{other._wrapper->clone()} {}
  AnySegmentIterator& operator=(const AnySegmentIterator& other) {
    if (this == &other) return *this;
    _wrapper = other._wrapper->clone();
    return *this;
  }

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() { _wrapper->increment(); }

  void advance(std::ptrdiff_t n) {
    DebugAssert(n >= 0, "Rewinding iterators is not implemented");
    _wrapper->advance(n);
  }

  bool equal(const AnySegmentIterator<T>& other) const { return _wrapper->equal(other._wrapper.get()); }

  std::ptrdiff_t distance_to(const AnySegmentIterator& other) const {
    return _wrapper->distance_to(other._wrapper.get());
  }

  SegmentPosition<T> dereference() const { return _wrapper->dereference(); }

 private:
  std::unique_ptr<opossum::detail::AnySegmentIteratorWrapperBase<T>> _wrapper;
};

}  // namespace opossum
