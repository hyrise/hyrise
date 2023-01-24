#pragma once

#include <tsl/sparse_map.h>

#include "flat_map_iterator.hpp"

namespace hyrise {

/**
 * Forward iterator that iterates over a tsl::sparse_map that maps a DataType to a vector of RowIDs. The
 * iteration process is as if the map would have been flattened and then iterated.
 *
 * @tparam DataType The key type of the underlying map.
 */
template <typename DataType>
class FlatMapIteratorImpl : public BaseFlatMapIteratorImpl {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = const RowID;
  using difference_type = std::ptrdiff_t;
  using pointer = const RowID*;
  using reference = const RowID&;

  using MapIterator = typename tsl::sparse_map<DataType, std::vector<RowID>>::const_iterator;

  explicit FlatMapIteratorImpl(MapIterator it);

  reference operator*() const final;

  FlatMapIteratorImpl& operator++() final;

  bool operator==(const BaseFlatMapIteratorImpl& other) const final;

  bool operator!=(const BaseFlatMapIteratorImpl& other) const final;

  std::unique_ptr<BaseFlatMapIteratorImpl> clone() const final;

  // Creates and returns an FlatMapIterator wrapping an instance of FlatMapIteratorImpl initialized using the passed
  // parameter.
  static FlatMapIterator flat_map_iterator(MapIterator it);

 private:
  MapIterator _map_iterator;
  size_t _vector_index;
};

}  // namespace hyrise
