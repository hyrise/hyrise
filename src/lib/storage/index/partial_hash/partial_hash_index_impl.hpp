#pragma once

#include <tsl/sparse_map.h>

#include <unordered_set>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/index/abstract_table_index.hpp"
#include "types.hpp"

namespace hyrise {

class AbstractSegment;
class PartialHashIndexTest;

/**
 * Forward iterator that iterates over a tsl::sparse_map that maps a DataType to a vector of RowIDs. The
 * iteration process is as if the map would have been flattened and then been iterated.
 *
 * @tparam DataType The key type of the underlying map.
 */
template <typename DataType>
class TableIndexIterator : public AbstractTableIndexIterator {
 public:
  using MapIterator = typename tsl::sparse_map<DataType, std::vector<RowID>>::const_iterator;

  explicit TableIndexIterator(MapIterator itr);

  reference operator*() const override;

  TableIndexIterator& operator++() override;

  bool operator==(const AbstractTableIndexIterator& other) const override;

  bool operator!=(const AbstractTableIndexIterator& other) const override;

  std::shared_ptr<AbstractTableIndexIterator> clone() const override;

  // Creates an instance of TableIndexIterator using the passed parameter and wraps that instance in an
  // IteratorWrapper. Return that wrapper.
  static IteratorWrapper wrap(MapIterator itr);

 private:
  MapIterator _map_iterator;
  size_t _vector_index;
};

/**
 * Forward iterator that iterates over a std::vector of RowIDs.
 */
class TableIndexNullValuesIterator : public AbstractTableIndexIterator {
 public:
  using MapIterator = typename std::vector<RowID>::const_iterator;

  explicit TableIndexNullValuesIterator(MapIterator itr);

  reference operator*() const override;

  TableIndexNullValuesIterator& operator++() override;

  bool operator==(const AbstractTableIndexIterator& other) const override;

  bool operator!=(const AbstractTableIndexIterator& other) const override;

  std::shared_ptr<AbstractTableIndexIterator> clone() const override;

  // Creates an instance of TableIndexNullValuesIterator using the passed parameter and wraps that instance in an
  // IteratorWrapper. Return that wrapper.
  static IteratorWrapper wrap(MapIterator itr);

 private:
  MapIterator _map_iterator;
};

/**
 * Base class that holds a PartialHashIndexImpl object with the correct resolved datatype.
 */
class BasePartialHashIndexImpl : public Noncopyable {
  friend PartialHashIndexTest;

 public:
  using Iterator = IteratorWrapper;
  using IteratorPair = std::pair<IteratorWrapper, IteratorWrapper>;

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

  size_t insert_entries(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID) override;
  size_t remove_entries(const std::vector<ChunkID>&) override;

  Iterator cbegin() const override;
  Iterator cend() const override;
  Iterator null_cbegin() const override;
  Iterator null_cend() const override;
  size_t estimate_memory_usage() const override;

  IteratorPair range_equals(const AllTypeVariant& value) const override;
  std::pair<IteratorPair, IteratorPair> range_not_equals(const AllTypeVariant& value) const override;

  std::unordered_set<ChunkID> get_indexed_chunk_ids() const override;

 private:
  tsl::sparse_map<DataType, std::vector<RowID>> _map;
  std::vector<RowID> _null_values;
  std::unordered_set<ChunkID> _indexed_chunk_ids = {};
};

}  // namespace hyrise
