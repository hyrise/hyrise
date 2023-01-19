#pragma once

#include <tsl/sparse_map.h>

#include "base_iterator.hpp"

namespace hyrise {

/**
 * Forward iterator that iterates over a tsl::sparse_map that maps a DataType to a vector of RowIDs. The
 * iteration process is as if the map would have been flattened and then iterated.
 *
 * @tparam DataType The key type of the underlying map.
 */
template <typename DataType>
class FlattenedMapIterator : public BaseIteratorImpl {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = const RowID;
  using difference_type = std::ptrdiff_t;
  using pointer = const RowID*;
  using reference = const RowID&;

  using MapIterator = typename tsl::sparse_map<DataType, std::vector<RowID>>::const_iterator;

  explicit FlattenedMapIterator(MapIterator it);

  reference operator*() const final;

  FlattenedMapIterator& operator++() final;

  bool operator==(const BaseIteratorImpl& other) const final;

  bool operator!=(const BaseIteratorImpl& other) const final;

  std::shared_ptr<BaseIteratorImpl> clone() const final;

  // Creates and returns an BaseIterator wrapping an instance of FlattenedMapIterator initialized using the passed
  // parameter.
  static BaseIterator iterator_wrapper(MapIterator it);

 private:
  MapIterator _map_iterator;
  size_t _vector_index;
};

}  // namespace hyrise
