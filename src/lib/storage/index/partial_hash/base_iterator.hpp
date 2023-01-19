#pragma once

namespace hyrise {

#include "types.hpp"

// Non-templated base class for the FlattenedMapIterator.
class BaseIteratorImpl {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = const RowID;
  using difference_type = std::ptrdiff_t;
  using pointer = const RowID*;
  using reference = const RowID&;

  BaseIteratorImpl(const BaseIteratorImpl& it) = default;
  BaseIteratorImpl() = default;
  virtual ~BaseIteratorImpl() = default;
  virtual reference operator*() const = 0;
  virtual BaseIteratorImpl& operator++() = 0;
  virtual bool operator==(const BaseIteratorImpl& other) const = 0;
  virtual bool operator!=(const BaseIteratorImpl& other) const = 0;
  virtual std::shared_ptr<BaseIteratorImpl> clone() const = 0;
};

/**
 * BaseIterator that implements an iterator interface and holds a pointer to an BasteIteratorImpl. This class
 * is required to allow runtime polymorphism without the need to directly pass pointers to iterators throughout the
 * codebase. It also provides copy construction and assignment facilities to easily duplicate other BaseIterators,
 * including their underlying implementation instances. This is especially important because the iterator type is a
 * forward iterator instead of a random access iterator, so if an iterator instance has to be retained before a
 * manipulating call, e.g., when calling it on std::distance, a copy has to be made beforehand.
 */
class BaseIterator {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = const RowID;
  using difference_type = std::ptrdiff_t;
  using pointer = const RowID*;
  using reference = const RowID&;

  explicit BaseIterator(std::shared_ptr<BaseIteratorImpl>&& index_iterator);
  BaseIterator(const BaseIterator& other);
  BaseIterator& operator=(const BaseIterator& other);
  reference operator*() const;
  BaseIterator& operator++();
  bool operator==(const BaseIterator& other) const;
  bool operator!=(const BaseIterator& other) const;

 private:
  std::shared_ptr<BaseIteratorImpl> _impl;
};

}  // namespace hyrise
