#pragma once

#include <utility>
#include <vector>

#include <tsl/sparse_map.h> // NOLINT
#include <tbb/concurrent_hash_map.h> // NOLINT

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/index/abstract_table_index.hpp"
#include "types.hpp"

namespace hyrise {

class AbstractSegment;
class PartialHashIndexTest;

/**
 * Forward iterator that iterates over a tsl::robin_map which maps a DataType to a vector of RowIDs. The iteration
 * process is as if the map would have been flattened and then been iterated.
 *
 * @tparam DataType The key type of the underlying map.
 */
template <typename DataType>
class TableIndexFlattenedSparseMapIterator : public BaseTableIndexIterator {
 public:
  using MapIteratorType = typename tbb::concurrent_hash_map<DataType, std::vector<RowID>>::const_iterator;

  explicit TableIndexFlattenedSparseMapIterator(MapIteratorType itr);

  reference operator*() const override;

  TableIndexFlattenedSparseMapIterator& operator++() override;

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
  using MapIteratorType = typename std::vector<RowID>::const_iterator;

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

  /**
   * Removes the given chunks from this index. If a chunk is not indexed, nothing will happen.
   *
   * @return The number of removed chunks.
   */
  virtual size_t remove_entries(const std::vector<ChunkID>&);

  virtual Iterator cbegin() const;
  virtual Iterator cend() const;
  virtual Iterator null_cbegin() const;
  virtual Iterator null_cend() const;
  virtual size_t memory_usage() const = 0;

  virtual IteratorPair range_equals(const AllTypeVariant& value) const;

  virtual std::pair<IteratorPair, IteratorPair> range_not_equals(const AllTypeVariant& value) const;

  virtual bool is_index_for(const ColumnID column_id) const;
  virtual std::unordered_set<ChunkID> get_indexed_chunk_ids() const;
};

/* Templated implementation of the PartialHashIndex. It is possible to index any immutable chunk of the indexed column.
 * Chunks can be added and removed using the insert_entries() and remove_entries() methods.
 */
template <typename DataType>
class PartialHashIndexImpl : public BasePartialHashIndexImpl {
  friend PartialHashIndexTest;

 public:
  PartialHashIndexImpl() = delete;
  PartialHashIndexImpl(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID);

  size_t insert_entries(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID) override;
  size_t remove_entries(const std::vector<ChunkID>&) override;

  Iterator cbegin() const override;
  Iterator cend() const override;
  Iterator null_cbegin() const override;
  Iterator null_cend() const override;
  size_t memory_usage() const override;

  IteratorPair range_equals(const AllTypeVariant& value) const override;
  std::pair<IteratorPair, IteratorPair> range_not_equals(const AllTypeVariant& value) const override;

  std::unordered_set<ChunkID> get_indexed_chunk_ids() const override;

 private:
  tbb::concurrent_hash_map<DataType, std::vector<RowID>> _map;
  std::vector<RowID> _null_values;
  std::unordered_set<ChunkID> _indexed_chunk_ids = {};
};

}  // namespace hyrise
