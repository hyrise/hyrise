#pragma once

#include "storage/table.hpp"
#include "tsl/robin_map.h"

namespace opossum {

class BaseTableIndexIterator : public std::iterator<std::forward_iterator_tag, const RowID> {
 public:
  virtual ~BaseTableIndexIterator() = default;

  virtual reference operator*() const { throw std::logic_error("cannot dereference on empty iterator"); }
  virtual BaseTableIndexIterator& operator++() { return *this; }
  virtual bool operator==(const BaseTableIndexIterator& other) const { return true; }
  virtual bool operator!=(const BaseTableIndexIterator& other) const { return false; }
};

template <typename DataType>
class TableIndexIterator : public BaseTableIndexIterator {
 public:
  using MapIteratorType = typename tsl::robin_map<DataType, std::vector<RowID>>::const_iterator;

  TableIndexIterator(MapIteratorType begin) : _map_iterator(begin), _vector_index(0) {}

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

 private:
  MapIteratorType _map_iterator;
  size_t _vector_index;
};

class IteratorWrapper : public std::iterator<std::forward_iterator_tag, const RowID> {
 public:
  IteratorWrapper(std::shared_ptr<BaseTableIndexIterator>&& ptr) : _impl(std::move(ptr)) {}

  reference operator*() const { return _impl->operator*(); }
  IteratorWrapper& operator++() {
    _impl->operator++();
    return *this;
  }
  bool operator==(const IteratorWrapper& other) const { return _impl->operator==(*other._impl); }
  bool operator!=(const IteratorWrapper& other) const { return _impl->operator!=(*other._impl); }

 private:
  std::shared_ptr<BaseTableIndexIterator> _impl;
};

class AbstractTableIndex : private Noncopyable {
 public:
  using Iterator = IteratorWrapper;

  /**
 * Predicts the memory consumption in bytes of creating this index.
 * See AbstractIndex::estimate_memory_consumption()
 * The introduction of PMR strings increased this significantly (in one test from 320 to 896). If you are interested
 * in reducing the memory footprint of these indexes, this is probably the first place you should look.
 */
  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

  //AbstractTableIndex() = delete;
  explicit AbstractTableIndex(const IndexType type);
  AbstractTableIndex(AbstractTableIndex&&) = default;
  virtual ~AbstractTableIndex() = default;

  std::pair<Iterator, Iterator> equals(const AllTypeVariant& value) const;

  Iterator cbegin() const { return _cbegin(); }

  Iterator cend() const { return _cend(); }

  Iterator null_cbegin() const { return _null_cbegin(); }

  Iterator null_cend() const { return _null_cend(); }

  IndexType type() const { return _type; }

  size_t memory_consumption() const {
    size_t bytes{0u};
    bytes += _memory_consumption();
    bytes += sizeof(std::vector<ChunkOffset>);  // _null_positions
    // bytes += sizeof(ChunkOffset) * _null_positions.capacity(); ToDo(pi) fix
    bytes += sizeof(_type);
    return bytes;
  }

  bool is_index_for(const ColumnID column_id) const;

  std::set<ChunkID> get_indexed_chunk_ids() const;

 protected:
  virtual Iterator _cbegin() const = 0;
  virtual Iterator _cend() const = 0;
  virtual Iterator _null_cbegin() const = 0;
  virtual Iterator _null_cend() const = 0;
  virtual std::pair<Iterator, Iterator> _equals(const AllTypeVariant& value) const = 0;
  virtual bool _is_index_for(const ColumnID column_id) const = 0;
  virtual std::set<ChunkID> _get_indexed_chunk_ids() const = 0;
  virtual size_t _memory_consumption() const = 0;

 private:
  const IndexType _type;
};

}  // namespace opossum
