#pragma once

#include "flat_map_iterator_impl.hpp"
#include "types.hpp"

/**
 * The choice of the concrete iterator implementation was subject to a long discussion (see PR 2448). The
 * PartialHashIndex uses the pointer to implementation (pImpl) idiom, i.e., the non-templated PartialHashIndex instance
 * holds an opaque pointer to its templated implementation PartialHashIndexImpl. Since the index access is done through
 * its iterators (more precisely, through its access methods that also work with iterators), a non-templated iterator is
 * also needed.
 *
 * We use the pImpl idiom to implement the FlatMapIterator as a non-templated class. Since FlatMapIteratorImpl is a
 * class template, we use BaseFlatMapIteratorImpl as a non-templated base class for the FlatMapIteratorImpl so that
 * FlatMapIterator can hold a pointer to a non-templated BaseFlatMapIteratorImpl instance. The PartialHashIndex's
 * iterator functions return (pairs of) FlatMapIterators so that PartialHashIndex itself can be non-templated.
 *
 *   +-----------------+            +-------------------------+
 *   |                 |            | <<Abstract>>            |
 *   | FlatMapIterator |----------->| BaseFlatMapIteratorImpl |
 *   |                 | unique_ptr |                         |
 *   +-----------------+            +-------------------------+
 *                                              ^
 *                                             /_\
 *                                              |    .......................
 *                                              |    : Template parameter: :
 *                                    +--------------: DataType            :
 *                                    |              :.....................:
 *                                    | FlatMapIteratorImpl  |
 *                                    |                      |
 *                                    +----------------------+
 *
 * The following design attempts were discarded:
 *
 * Iterator as a template parameter: Using templated iterators as a template decreased the code quality significantly.
 * Also, static and dynamic polymorphism do not go well together because virtual functions cannot be templated.
 *
 * Returning BaseFlatMapIteratorImpl pointers rather than FlatMapIterator instances: This solution attempt has also led
 * to a decreased code quality due to the ongoing need to dereference every pointer. Moreover, to make the iterators
 * work with algorithms (e.g. std::distance) it was necessary to cast the pointers from BaseFlatMapIteratorImpl to
 * FlatMapIteratorImpl. This is important because the iterator type is a forward iterator instead of a random access
 * iterator, so if an iterator instance has to be retained before a manipulating call, a copy has to be made beforehand.
 * Because (copy) constructors can not be virtual, runtime polymorphism does not work in this situation.
 */

namespace hyrise {

/**
 * FlatMapIterator that implements an iterator interface and holds a pointer to an BasteIteratorImpl. This class
 * is required to allow runtime polymorphism without the need to directly pass pointers to iterators throughout the
 * codebase. It also provides copy construction and assignment facilities to easily duplicate other FlatMapIterators,
 * including their underlying implementation FlatMapIteratorImpl. This is especially important because the iterators
 * type is a forward iterator instead of a random access iterator, so if an iterator instance has to be retained before
 * a manipulating call, e.g., when calling it on std::distance, a copy has to be made beforehand.
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
