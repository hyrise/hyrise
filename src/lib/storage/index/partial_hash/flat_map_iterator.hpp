#pragma once

#include "types.hpp"

/**
 * The choice of the concrete iterator implementation was subject to long discussion. The PartialHashIndex uses the
 * pointer to implementation (pImpl) idom, that means  i.e. a non-templated class PartialHashIndex holds an opaque
 * pointer to its templated implementation PartialHashIndexImpl. Since the index access is done through its iterators
 * (more precisely through its access methods that also work with iterators), a non-templated iterator was also needed.
 *
 * Several attempts were made to achieve this, e.g. passing the iterator to the index as a template, keeping the
 * templated iterators inside the IndexImpl and access them via the index's access methods. However, these attempts
 * were not only very ugly, it was also not possible to archive the desirend effect because templatization and class
 * polymorphism do not got well together.
 *
 * In the end we decided to use the pImpl idom also for the iterators: Since FlatMapIteratorImpl is a class template,
 * we use BaseFlatMapIteratorImpl as a non-templated base class for the FlatMapIteratorImpl. We implemented
 * the FlatMapIterator holding a pointer to a BaseFlatMapIteratorImpl instance. Using a BaseFlatMapIteratorImpl pointer
 * rather than a FlatMapIteratorImpl, FlatMapIterator itself does not have to be templated. The PartialHashIndex's
 * iterator functions then return (pairs of) FlatMapIterators so that PartialHashIndex can be non-templated.
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
