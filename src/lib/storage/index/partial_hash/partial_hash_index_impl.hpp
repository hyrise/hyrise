#pragma once

#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "tsl/robin_map.h"
#include "types.hpp"

namespace opossum {

class AbstractSegment;
class PartialHashIndexTest;

/**
 * Base class that holds a PartialHashIndexImpl object with the correct unboxed datatype.
 */
class BasePartialHashIndexImpl : public Noncopyable {
  friend PartialHashIndexTest;

 public:
  using Iterator = AbstractTableIndex::Iterator;
  using IteratorPair = std::pair<AbstractTableIndex::Iterator, AbstractTableIndex::Iterator>;

  virtual ~BasePartialHashIndexImpl() = default;

  virtual size_t add(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID) { return 0; }
  virtual size_t remove(const std::vector<ChunkID>&) { return 0; }

  virtual Iterator cbegin() const { return Iterator(std::make_shared<BaseTableIndexIterator>()); }
  virtual Iterator cend() const { return Iterator(std::make_shared<BaseTableIndexIterator>()); }
  virtual Iterator null_cbegin() const { return Iterator(std::make_shared<BaseTableIndexIterator>()); }
  virtual Iterator null_cend() const { return Iterator(std::make_shared<BaseTableIndexIterator>()); }
  virtual size_t memory_consumption() const { return 0; }

  virtual IteratorPair equals(const AllTypeVariant& value) const {
    return std::make_pair(Iterator(std::make_shared<BaseTableIndexIterator>()),
                          Iterator(std::make_shared<BaseTableIndexIterator>()));
  }

  virtual std::pair<IteratorPair, IteratorPair> not_equals(const AllTypeVariant& value) const {
    return std::make_pair(equals(value), equals(value));
  }

  virtual bool is_index_for(const ColumnID column_id) const { return false; }
  virtual std::set<ChunkID> get_indexed_chunk_ids() const { return std::set<ChunkID>(); }
};

/* Implementation of a partial hash index, that can index any chunk in a column. You can add and remove chunks to
 * the index using the add and remove methods. This index can drastically improve the performance of an IndexJoin
 * operation.
 */
template <typename DataType>
class PartialHashIndexImpl : public BasePartialHashIndexImpl {
  friend PartialHashIndexTest;

 public:
  PartialHashIndexImpl() = delete;
  PartialHashIndexImpl(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID);

  size_t add(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID) override;
  size_t remove(const std::vector<ChunkID>&) override;

  Iterator cbegin() const override;
  Iterator cend() const override;
  Iterator null_cbegin() const override;
  Iterator null_cend() const override;
  size_t memory_consumption() const override;

  IteratorPair equals(const AllTypeVariant& value) const override;
  std::pair<IteratorPair, IteratorPair> not_equals(const AllTypeVariant& value) const override;

  // returns sorted array
  std::set<ChunkID> get_indexed_chunk_ids() const override;

 private:
  tsl::robin_map<DataType, std::vector<RowID>> _map;
  //TODO(pi): write documentation!
  tsl::robin_map<bool, std::vector<RowID>> _null_values;
  std::set<ChunkID> _indexed_chunk_ids = {};
};

}  // namespace opossum
