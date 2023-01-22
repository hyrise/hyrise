#pragma once

#include <tsl/sparse_map.h>

#include <unordered_set>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "flat_map_iterator.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"

namespace hyrise {

using Iterator = FlatMapIterator;
using IteratorPair = std::pair<Iterator, Iterator>;

class PartialHashIndexTest;

/**
 * Base class that holds a PartialHashIndexImpl object with the correct resolved datatype.
 */
class BasePartialHashIndexImpl : public Noncopyable {
  friend PartialHashIndexTest;

 public:
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

  virtual bool indexed_null_values() const = 0;

  virtual Iterator cbegin() const = 0;
  virtual Iterator cend() const = 0;
  virtual Iterator null_cbegin() const = 0;
  virtual Iterator null_cend() const = 0;
  virtual size_t estimate_memory_usage() const = 0;

  virtual IteratorPair range_equals(const AllTypeVariant& value) const = 0;

  virtual std::pair<IteratorPair, IteratorPair> range_not_equals(const AllTypeVariant& value) const = 0;

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

  size_t insert_entries(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID) final;
  size_t remove_entries(const std::vector<ChunkID>&) final;

  bool indexed_null_values() const final;

  Iterator cbegin() const final;
  Iterator cend() const final;
  Iterator null_cbegin() const final;
  Iterator null_cend() const final;
  size_t estimate_memory_usage() const final;

  IteratorPair range_equals(const AllTypeVariant& value) const final;
  std::pair<IteratorPair, IteratorPair> range_not_equals(const AllTypeVariant& value) const final;

  std::unordered_set<ChunkID> get_indexed_chunk_ids() const final;

 private:
  tsl::sparse_map<DataType, std::vector<RowID>> _map;
  tsl::sparse_map<DataType, std::vector<RowID>> _null_values;
  std::unordered_set<ChunkID> _indexed_chunk_ids = {};
};

}  // namespace hyrise
