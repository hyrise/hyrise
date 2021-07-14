#pragma once

#include <utility>
#include <vector>

#include <storage/chunk.hpp>
#include "all_type_variant.hpp"
#include "tsl/robin_map.h"
#include "types.hpp"

namespace opossum {

class AbstractSegment;

class BasePartialHashIndexImpl : public Noncopyable {
  using Iterator = std::vector<RowID>::const_iterator;

 public:
  virtual ~BasePartialHashIndexImpl() = default;

  virtual Iterator cbegin() const = 0;
  virtual Iterator cend() const = 0;
  virtual std::vector<std::shared_ptr<const AbstractSegment>> get_indexed_segments() const = 0;
  virtual size_t memory_consumption() const = 0;

  virtual std::pair<Iterator, Iterator> equals(const AllTypeVariant& value) const = 0;

  virtual bool is_index_for(const ColumnID column_id) const = 0;
  virtual std::set<ChunkID> get_indexed_chunk_ids() const = 0;

  virtual void change_indexed_column(const ColumnID column_id) = 0;
};

template <typename DataType>
class PartialHashIndexImpl : public BasePartialHashIndexImpl {
 public:
  using Iterator = std::vector<RowID>::const_iterator;

  PartialHashIndexImpl() = delete;
  PartialHashIndexImpl(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID,
                       std::vector<RowID>& null_positions);

  Iterator cbegin() const override;
  Iterator cend() const override;
  std::vector<std::shared_ptr<const AbstractSegment>> get_indexed_segments() const override;
  size_t memory_consumption() const override;

  std::pair<Iterator, Iterator> equals(const AllTypeVariant& value) const override;

  bool is_index_for(const ColumnID column_id) const override;
  // returns sorted array
  std::set<ChunkID> get_indexed_chunk_ids() const override;
 private:
  tsl::robin_map<DataType , std::vector<RowID>> _map;

  // TODO(pi): Decide whether we store column id here or use tablestatistics on the table
  ColumnID _column_id;
  std::vector<RowID> _row_ids;
  std::set<ChunkID> _indexed_chunk_ids;  // constant time lookup
  std::vector<std::shared_ptr<const AbstractSegment>> _indexed_segments;
};

}  // namespace opossum
