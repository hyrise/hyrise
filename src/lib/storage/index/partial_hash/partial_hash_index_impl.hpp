#pragma once

#include <tsl/sparse_map.h>

#include <unordered_set>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"

namespace hyrise {

class PartialHashIndexTest;

/**
 * Forward iterator that iterates over a tsl::sparse_map that maps a DataType to a vector of RowIDs. The
 * iteration process is as if the map would have been flattened and then iterated.
 *
 * @tparam DataType The key type of the underlying map.
 */
template <typename DataType>
class TableIndexIterator {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = const RowID;
  using difference_type = std::ptrdiff_t;
  using pointer = const RowID*;
  using reference = const RowID&;

  using MapIterator = typename tsl::sparse_map<DataType, std::vector<RowID>>::const_iterator;

  explicit TableIndexIterator(MapIterator it);

  reference operator*() const;

  TableIndexIterator& operator++();

  bool operator==(const AbstractTableIndexIterator& other) const;

  bool operator!=(const AbstractTableIndexIterator& other) const;

  std::shared_ptr<AbstractTableIndexIterator> clone() const;

 private:
  MapIterator _map_iterator;
  size_t _vector_index;
};

/**
 * Base class that holds a PartialHashIndexImpl object with the correct resolved datatype.
 */
class BasePartialHashIndexImpl : public Noncopyable {
  friend PartialHashIndexTest;

 public:
  using Iterator = TableIndexIterator<DataType>;
  using IteratorPair = std::pair<Iterator, Iterator>;

  virtual ~BasePartialHashIndexImpl() = default;

  /**
   * Adds the given chunks to this index. If a chunk is already indexed, it is not indexed again.
   *
   * @return The number of added chunks.
   */
  virtual size_t insert_entries(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID) = 0;

  /**
   * Removes the given chunks from this index. If a chunk is not indexed, nothing will happen.
   *
   * @return The number of removed chunks.
   */
  virtual size_t remove_entries(const std::vector<ChunkID>&) = 0;

  // virtual Iterator cbegin() const = 0;
  // virtual Iterator cend() const = 0;
  // virtual Iterator null_cbegin() const = 0;
  // virtual Iterator null_cend() const = 0;
  virtual size_t estimate_memory_usage() const = 0;

  // virtual IteratorPair range_equals(const AllTypeVariant& value) const = 0;

  // virtual std::pair<IteratorPair, IteratorPair> range_not_equals(const AllTypeVariant& value) const = 0;

  virtual std::unordered_set<ChunkID> get_indexed_chunk_ids() const = 0;
};

/* Templated implementation of the PartialHashIndex. It is possible to index any immutable chunk of the indexed column.
 * Chunks can be added via `insert_entries()`.
 *
 * We did not perform a comprehensive experimental evaluation of different hash maps for concurrent workloads
 * (including reads and modifications) yet. Thus, choosing a different hash map might result in better performance.
 */
template <typename DataType>
class PartialHashIndexImpl : public BasePartialHashIndexImpl {
  friend PartialHashIndexTest;

 public:
  PartialHashIndexImpl() = delete;
  PartialHashIndexImpl(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID);

  size_t insert_entries(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID);
  size_t remove_entries(const std::vector<ChunkID>&);

  TableIndexIterator<DataType> cbegin() const;
  Iterator cend() const;
  Iterator null_cbegin() const;
  Iterator null_cend() const;
  size_t estimate_memory_usage() const;

  IteratorPair range_equals(const AllTypeVariant& value) const;
  std::pair<IteratorPair, IteratorPair> range_not_equals(const AllTypeVariant& value) const;

  std::unordered_set<ChunkID> get_indexed_chunk_ids() const;

 private:
  tsl::sparse_map<DataType, std::vector<RowID>> _map;
  tsl::sparse_map<DataType, std::vector<RowID>> _null_values;
  std::unordered_set<ChunkID> _indexed_chunk_ids = {};
};

}  // namespace hyrise
