#pragma once

#include <utility>
#include <vector>

#include <tbb/concurrent_unordered_map.h>  // NOLINT linter identifies this file as a C system header.
#include <tbb/concurrent_vector.h>         // NOLINT linter identifies this file as a C header.

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/index/abstract_table_index.hpp"
#include "types.hpp"

namespace hyrise {

class AbstractSegment;
class PartialHashIndexTest;

/**
 * Forward iterator that iterates over a tbb::concurrent_unordered_map that maps a DataType to a vector of RowIDs. The
 * iteration process is as if the map would have been flattened and then been iterated.
 *
 * @tparam DataType The key type of the underlying map.
 */
template <typename DataType>
class TableIndexTbbUnorderedMapIterator : public BaseTableIndexIterator {
 public:
  using MapIteratorType = typename tbb::concurrent_unordered_map<DataType, tbb::concurrent_vector<RowID>>::const_iterator;

  explicit TableIndexTbbUnorderedMapIterator(MapIteratorType itr);

  reference operator*() const override;

  TableIndexTbbUnorderedMapIterator& operator++() override;

  bool operator==(const BaseTableIndexIterator& other) const override;

  bool operator!=(const BaseTableIndexIterator& other) const override;

  std::shared_ptr<BaseTableIndexIterator> clone() const override;

 private:
  MapIteratorType _map_iterator;
  size_t _vector_index;
};

/**
 * Forward iterator that iterates over a std::vector of RowIDs.
 */
class TableIndexVectorIterator : public BaseTableIndexIterator {
 public:
  using MapIteratorType = typename tbb::concurrent_vector<RowID>::const_iterator;

  explicit TableIndexVectorIterator(MapIteratorType itr);

  reference operator*() const override;

  TableIndexVectorIterator& operator++() override;

  bool operator==(const BaseTableIndexIterator& other) const override;

  bool operator!=(const BaseTableIndexIterator& other) const override;

  std::shared_ptr<BaseTableIndexIterator> clone() const override;

 private:
  MapIteratorType _map_iterator;
};

/**
 * Base class that holds a PartialHashIndexImpl object with the correct resolved datatype.
 */
class BasePartialHashIndexImpl : public Noncopyable {
  friend PartialHashIndexTest;

 public:
  using Iterator = AbstractTableIndex::Iterator;
  using IteratorPair = std::pair<AbstractTableIndex::Iterator, AbstractTableIndex::Iterator>;

  virtual ~BasePartialHashIndexImpl() = default;

  /**
   * Adds the given chunks to this index. If a chunk is already indexed, it is not indexed again.
   *
   * @return The number of added chunks.
   */
  virtual size_t insert_entries(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID);

  virtual Iterator cbegin() const;
  virtual Iterator cend() const;
  virtual Iterator null_cbegin() const;
  virtual Iterator null_cend() const;
  virtual size_t estimate_memory_usage() const = 0;

  virtual IteratorPair range_equals(const AllTypeVariant& value) const;

  virtual std::pair<IteratorPair, IteratorPair> range_not_equals(const AllTypeVariant& value) const;

  virtual bool is_index_for(const ColumnID column_id) const;
  virtual tbb::concurrent_unordered_set<ChunkID> get_indexed_chunk_ids() const;
};

/* Templated implementation of the PartialHashIndex. It is possible to index any immutable chunk of the indexed column.
 * Chunks can be added via `insert_entries()`.
 *
 * For now, we do not support removing entries for a given chunk id. Thus, if previously inserted index entries for a
 * given chunk are to be removed, the index has to be re-created for the desired set of chunks. This is a current
 * simplification and limitation to enable concurrent access using the `tbb::concurrent_unordered_map`, which supports
 * concurrent insertion, lookup, and traversal, but does not support concurrent erasure. If one wants to use a different
 * hash map, as well as shared and unique locks for enabling thread-safe operations, one has to make sure that the hash
 * map is locked until the read and write accesses are complete. Since this index implementation's operations return
 * iterators, only locking while retrieving the iterators would not suffice since another thread might modify the hash
 * map and, thus, invalidate (depending on the hash map implementation) the retreived iterators with modifications.
 * For further notes and discussions, please review PR #2448.
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

  size_t insert_entries(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID) override;

  Iterator cbegin() const override;
  Iterator cend() const override;
  Iterator null_cbegin() const override;
  Iterator null_cend() const override;
  size_t estimate_memory_usage() const override;

  IteratorPair range_equals(const AllTypeVariant& value) const override;
  std::pair<IteratorPair, IteratorPair> range_not_equals(const AllTypeVariant& value) const override;

  tbb::concurrent_unordered_set<ChunkID> get_indexed_chunk_ids() const override;

 private:
  tbb::concurrent_unordered_map<DataType, tbb::concurrent_vector<RowID>> _map;
  tbb::concurrent_vector<RowID> _null_values;
  tbb::concurrent_unordered_set<ChunkID> _indexed_chunk_ids = {};
  std::mutex _insert_entries_mutex;
};

}  // namespace hyrise
