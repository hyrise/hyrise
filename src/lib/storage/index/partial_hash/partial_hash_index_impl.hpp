#pragma once

#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/index/abstract_table_index.hpp"
#include "tsl/robin_map.h"
#include "types.hpp"

namespace opossum {

class AbstractSegment;
class PartialHashIndexTest;

/**
 * Base class that holds a PartialHashIndexImpl object with the correct resolved datatype.
 */
class BasePartialHashIndexImpl : public Noncopyable {
  friend PartialHashIndexTest;

 public:
  using Iterator = AbstractTableIndex::Iterator;
  using IteratorPair = std::pair<AbstractTableIndex::Iterator, AbstractTableIndex::Iterator>;

  virtual ~BasePartialHashIndexImpl() = default;

  virtual size_t insert_entries(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID);
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
  tsl::robin_map<DataType, std::vector<RowID>> _map;
  // We construct a map for NULL-values here to make use of the same iterator type on values and NULL-values.
  tsl::robin_map<bool, std::vector<RowID>> _null_values;
  std::set<ChunkID> _indexed_chunk_ids = {};
};

}  // namespace opossum
