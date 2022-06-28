#pragma once

#include <tsl/sparse_map.h>

#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/index/abstract_table_index.hpp"
#include "types.hpp"

namespace opossum {

class AbstractSegment;
class PartialHashIndexTest;

/**
 * Forward iterator that iterates over a tsl::robin_map which maps a DataType to a vector of RowIDs. The iteration
 * process is as if the map would have been flattened and then been iterated.
 *
 * @tparam DataType The key type of the underlying map.
 */
template <typename DataType>
class TableIndexIterator : public BaseTableIndexIterator {
 public:
  using MapIteratorType = typename tsl::sparse_map<DataType, std::vector<RowID>>::const_iterator;

  explicit TableIndexIterator(MapIteratorType itr) : _map_iterator(itr), _vector_index(0) {}

  reference operator*() const override { return _map_iterator->second[_vector_index]; }

  TableIndexIterator& operator++() override {
    if (++_vector_index >= _map_iterator->second.size()) {
      _map_iterator++;
      _vector_index = 0;
    }
    return *this;
  }

  bool operator==(const BaseTableIndexIterator& other) const override {
    auto obj = dynamic_cast<const TableIndexIterator*>(&other);
    return obj && _map_iterator == obj->_map_iterator && _vector_index == obj->_vector_index;
  }

  bool operator!=(const BaseTableIndexIterator& other) const override {
    auto obj = dynamic_cast<const TableIndexIterator*>(&other);
    return !obj || _map_iterator != obj->_map_iterator || _vector_index != obj->_vector_index;
  }

  std::shared_ptr<BaseTableIndexIterator> clone() const override {
    return std::make_shared<TableIndexIterator<DataType>>(*this);
  }

 private:
  MapIteratorType _map_iterator;
  size_t _vector_index;
};

class TableIndexNullIterator : public BaseTableIndexIterator {
 public:
  using MapIteratorType = typename std::vector<RowID>::const_iterator;

  explicit TableIndexNullIterator(MapIteratorType itr) : _map_iterator(itr) {}

  reference operator*() const override { return *_map_iterator; }

  TableIndexNullIterator& operator++() override {
    _map_iterator++;
    return *this;
  }

  bool operator==(const BaseTableIndexIterator& other) const override {
    auto obj = dynamic_cast<const TableIndexNullIterator*>(&other);
    return obj && _map_iterator == obj->_map_iterator;
  }

  bool operator!=(const BaseTableIndexIterator& other) const override {
    auto obj = dynamic_cast<const TableIndexNullIterator*>(&other);
    return !obj || _map_iterator != obj->_map_iterator;
  }

  std::shared_ptr<BaseTableIndexIterator> clone() const override {
    return std::make_shared<TableIndexNullIterator>(*this);
  }

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
  virtual size_t memory_consumption() const;

  virtual IteratorPair range_equals(const AllTypeVariant& value) const;

  virtual std::pair<IteratorPair, IteratorPair> range_not_equals(const AllTypeVariant& value) const;

  virtual bool is_index_for(const ColumnID column_id) const;
  virtual std::set<ChunkID> get_indexed_chunk_ids() const;
};

/* Implementation of a partial hash index, that can index any chunk in a column. You can add and remove chunks to
 * the index using the insert_entries and remove_entries methods.
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
  size_t memory_consumption() const override;

  IteratorPair range_equals(const AllTypeVariant& value) const override;
  std::pair<IteratorPair, IteratorPair> range_not_equals(const AllTypeVariant& value) const override;

  std::set<ChunkID> get_indexed_chunk_ids() const override;

 private:
  tsl::sparse_map<DataType, std::vector<RowID>> _map;
  // We construct a map for NULL-values here to make use of the same iterator type on values and NULL-values.
  std::vector<RowID> _null_values;
  std::set<ChunkID> _indexed_chunk_ids = {};
};

}  // namespace opossum
