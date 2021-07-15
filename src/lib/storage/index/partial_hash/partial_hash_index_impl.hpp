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

class BasePartialHashIndexImpl : public Noncopyable {
  friend PartialHashIndexTest;

 public:
  using Iterator = AbstractTableIndex::Iterator;

  virtual ~BasePartialHashIndexImpl() = default;

  virtual Iterator cbegin() const { return Iterator(std::make_shared<BaseTableIndexIterator>()); }
  virtual Iterator cend() const { return Iterator(std::make_shared<BaseTableIndexIterator>()); }
  virtual Iterator null_cbegin() const { return Iterator(std::make_shared<BaseTableIndexIterator>()); }
  virtual Iterator null_cend() const { return Iterator(std::make_shared<BaseTableIndexIterator>()); }
  virtual size_t memory_consumption() const { return 0; }

  virtual std::pair<Iterator, Iterator> equals(const AllTypeVariant& value) const {
    return std::make_pair(Iterator(std::make_shared<BaseTableIndexIterator>()),
                          Iterator(std::make_shared<BaseTableIndexIterator>()));
  }

  virtual bool is_index_for(const ColumnID column_id) const { return false; }
  virtual std::set<ChunkID> get_indexed_chunk_ids() const { return std::set<ChunkID>(); }
};

template <typename DataType>
class PartialHashIndexImpl : public BasePartialHashIndexImpl {
  friend PartialHashIndexTest;

 public:
  PartialHashIndexImpl() = delete;
  PartialHashIndexImpl(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID);

  Iterator cbegin() const override;
  Iterator cend() const override;
  Iterator null_cbegin() const override;
  Iterator null_cend() const override;
  size_t memory_consumption() const override;

  std::pair<Iterator, Iterator> equals(const AllTypeVariant& value) const override;

  bool is_index_for(const ColumnID column_id) const override;
  // returns sorted array
  std::set<ChunkID> get_indexed_chunk_ids() const override;

 private:
  tsl::robin_map<DataType, std::vector<RowID>> _map;
  tsl::robin_map<bool, std::vector<RowID>> _null_values;

  // TODO(pi): Decide whether we store column id here or use tablestatistics on the table
  ColumnID _column_id;
  std::set<ChunkID> _indexed_chunk_ids = {};  // constant time lookup
};

}  // namespace opossum
