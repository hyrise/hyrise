#pragma once

#include <memory>

#include "storage/column_iterables/base_column_iterators.hpp"

namespace opossum {

namespace detail {

/**
 * Emulates a base class for column iterators with a virtual interface.
 * It duplicates all methods implemented by column iterators as part of
 * a virtual interface.
 */
template <typename T>
class AnyColumnIteratorWrapperBase {
 public:
  virtual ~AnyColumnIteratorWrapperBase() = default;

  virtual void increment() = 0;
  virtual bool equal(const AnyColumnIteratorWrapperBase<T>* other) const = 0;
  virtual ColumnIteratorValue<T> dereference() const = 0;

  /**
   * Column iterators need to be copyable so we need a way
   * to copy the iterator within the wrapper.
   */
  virtual std::unique_ptr<AnyColumnIteratorWrapperBase<T>> clone() const = 0;
};

/**
 * @brief The class where the wrapped iterator’s methods are called.
 *
 * Passes the virtual method call on to the non-virtual methods of the
 * iterator class passed as template argument.
 */
template <typename T, typename Iterator>
class AnyColumnIteratorWrapper : public AnyColumnIteratorWrapperBase<T> {
 public:
  explicit AnyColumnIteratorWrapper(const Iterator& iterator) : _iterator{iterator} {}

  void increment() final { ++_iterator; }

  /**
   * Although `other` could have a different type, it is practically impossible,
   * since AnyColumnIterator is only used within AnyColumnIterable.
   */
  bool equal(const AnyColumnIteratorWrapperBase<T>* other) const final {
    const auto casted_other = static_cast<const AnyColumnIteratorWrapper<T, Iterator>*>(other);
    return _iterator == casted_other->_iterator;
  }

  ColumnIteratorValue<T> dereference() const final {
    const auto value = *_iterator;
    return {value.value(), value.is_null(), value.chunk_offset()};
  }

  std::unique_ptr<AnyColumnIteratorWrapperBase<T>> clone() const final {
    return std::make_unique<AnyColumnIteratorWrapper<T, Iterator>>(_iterator);
  }

 private:
  Iterator _iterator;
};

}  // namespace detail

template <typename IterableT>
class AnyColumnIterable;

/**
 * @brief Erases the type of any column iterator
 *
 * Erases the type of any column iterator by wrapping it
 * in a templated class inheriting from a common base class.
 * The base class specifies a virtual interface which is
 * implemented by the templated sub-class.
 *
 * AnyColumnIterator inherits from BaseColumnIterator and
 * thus has the same interface as all other column iterators.
 *
 * AnyColumnIterator exists only to improve compile times and should
 * not be used outside of AnyColumnIterable.
 *
 * For another example for type erasure see: https://en.wikibooks.org/wiki/More_C%2B%2B_Idioms/Type_Erasure
 */
template <typename T>
class AnyColumnIterator : public BaseColumnIterator<AnyColumnIterator<T>, ColumnIteratorValue<T>> {
 private:
  /**
   * Prevents AnyColumnIterator from being created
   * by anything else but AnyColumnIterable
   *
   * @{
   */
  template <typename U>
  friend class AnyColumnIterable;

  template <typename Iterator>
  explicit AnyColumnIterator(const Iterator& iterator)
      : _wrapper{std::make_unique<detail::AnyColumnIteratorWrapper<T, Iterator>>(iterator)} {}
  /**@}*/

 public:
  AnyColumnIterator(const AnyColumnIterator& other) : _wrapper{other._wrapper->clone()} {}

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() { _wrapper->increment(); }
  bool equal(const AnyColumnIterator<T>& other) const { return _wrapper->equal(other._wrapper.get()); }
  ColumnIteratorValue<T> dereference() const { return _wrapper->dereference(); }

 private:
  std::unique_ptr<detail::AnyColumnIteratorWrapperBase<T>> _wrapper;
};

}  // namespace opossum
