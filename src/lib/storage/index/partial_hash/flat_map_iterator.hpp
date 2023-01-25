#pragma once

#include "types.hpp"

/**
 * The choice of the concrete iterator implementation was subject to a long discussion. The PartialHashIndex uses the
 * pointer to implementation (pImpl) idiom, i.e., the non-templated PartialHashIndex instance holds an opaque pointer
 * to its templated implementation PartialHashIndexImpl. Since the index access is done through its iterators (more
 * precisely, through its access methods that also work with iterators), a non-templated iterator is also needed.
 *
 *
 * The following design attempts were discarded:
 *
 * Iterator as a template parameter: Using templated iterators as a template decreased the code quality significantly.
 * Also, static & compile-time polymorphism do not go well together because virtual functions can not be templated.
 *
 * Returning BaseFlatMapIteratorImpl pointers rather than FlatMapIterator instances: Since the methods of the
 * FlatMapIteratorImpl must be accessed, it would be necessary to cast the pointers to every time the index is
 * accessed.
 *
 *
 * We use the pImpl idiom to implement the FlatMapIterator as a non-templated class. Since FlatMapIteratorImpl is a
 * class template, we use BaseFlatMapIteratorImpl as a non-templated base class for the FlatMapIteratorImpl so that
 * FlatMapIterator can hold a pointer to a non-templated BaseFlatMapIteratorImpl instance. The PartialHashIndex's
 * iterator functions return (pairs of) FlatMapIterators so that PartialHashIndex itself can be non-templated.
 *
 *
 *       +-----------------+              +-------------------------+ 
 *       | FlatMapIterator |              | BaseFlatMapIteratorImpl |
 *       |                 | ------------>|                         |
 *       | (not templated) |  unique_ptr  |     (not templated)     |
 *       +-----------------+              +-------------------------+
 *                                                     |
 *                                                     | inheritance
 *                                                     v
 *                                          +---------------------+ 
 *                                          | FlatMapIteratorImpl |
 *                                          |                     |
 *                                          |   (class template)  |
 *                                          +---------------------+  
 */

namespace hyrise {

// Non-templated base class for the FlatMapIteratorImpl.
class BaseFlatMapIteratorImpl {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = const RowID;
  using difference_type = std::ptrdiff_t;
  using pointer = const RowID*;
  using reference = const RowID&;

  BaseFlatMapIteratorImpl(const BaseFlatMapIteratorImpl& it) = default;
  BaseFlatMapIteratorImpl() = default;
  virtual ~BaseFlatMapIteratorImpl() = default;
  virtual reference operator*() const = 0;
  virtual BaseFlatMapIteratorImpl& operator++() = 0;
  virtual bool operator==(const BaseFlatMapIteratorImpl& other) const = 0;
  virtual bool operator!=(const BaseFlatMapIteratorImpl& other) const = 0;
  virtual std::unique_ptr<BaseFlatMapIteratorImpl> clone() const = 0;
};

/**
 * FlatMapIterator that implements an iterator interface and holds a pointer to an BasteIteratorImpl. This class
 * is required to allow runtime polymorphism without the need to directly pass pointers to iterators throughout the
 * codebase. It also provides copy construction and assignment facilities to easily duplicate other BaseIterators,
 * including their underlying implementation instances. This is especially important because the iterator type is a
 * forward iterator instead of a random access iterator, so if an iterator instance has to be retained before a
 * manipulating call, e.g., when calling it on std::distance, a copy has to be made beforehand.
 */
class FlatMapIterator {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = const RowID;
  using difference_type = std::ptrdiff_t;
  using pointer = const RowID*;
  using reference = const RowID&;

  explicit FlatMapIterator(std::unique_ptr<BaseFlatMapIteratorImpl>&& index_iterator);
  FlatMapIterator(const FlatMapIterator& other);
  FlatMapIterator& operator=(const FlatMapIterator& other);
  reference operator*() const;
  FlatMapIterator& operator++();
  bool operator==(const FlatMapIterator& other) const;
  bool operator!=(const FlatMapIterator& other) const;

 private:
  std::unique_ptr<BaseFlatMapIteratorImpl> _impl;
};

}  // namespace hyrise
