#pragma once

#include <memory>

#include "storage/column_iterables/base_column_iterators.hpp"

namespace opossum {

namespace detail {

template <typename T>
class AnyColumnIteratorWrapperBase {
 public:
  virtual ~AnyColumnIteratorWrapperBase() = default;

  virtual void increment() = 0;
  virtual bool equal(const AnyColumnIteratorWrapperBase<T>* other) const = 0;
  virtual ColumnIteratorValue<T> dereference() const = 0;
  virtual std::unique_ptr<AnyColumnIteratorWrapperBase<T>> clone() const = 0;
};

template <typename T, typename Iterator>
class AnyColumnIteratorWrapper : public AnyColumnIteratorWrapperBase<T> {
 public:
  explicit AnyColumnIteratorWrapper(const Iterator& iterator) : _iterator{iterator} {}

  void increment() final { ++_iterator; }

  bool equal(const AnyColumnIteratorWrapperBase<T>* other) const final {
    const auto casted_other = dynamic_cast<const AnyColumnIteratorWrapper<T, Iterator>*>(other);

    if (casted_other == nullptr) return false;

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

/**
 * @brief Erases the type of any column iterator
 */
template <typename T>
class AnyColumnIterator : public BaseColumnIterator<AnyColumnIterator<T>, ColumnIteratorValue<T>> {
 public:
  template <typename Iterator>
  explicit AnyColumnIterator(const Iterator& iterator)
      : _wrapper{std::make_unique<detail::AnyColumnIteratorWrapper<T, Iterator>>(iterator)} {}

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
