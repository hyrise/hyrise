#include "partial_hash_index.hpp"

#include <tsl/sparse_set.h>

#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

PartialHashIndex::PartialHashIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index,
                                   const ColumnID column_id)
    : _column_id{column_id} {
  Assert(!chunks_to_index.empty(), "PartialHashIndex requires chunks_to_index not to be empty.");
  resolve_data_type(chunks_to_index.front().second->get_segment(column_id)->data_type(),
                    [&](const auto column_data_type) {
                      using ColumnDataType = typename decltype(column_data_type)::type;
                      _impl = std::make_unique<PartialHashIndexImpl<ColumnDataType>>(chunks_to_index, _column_id);
                    });
}

size_t PartialHashIndex::insert(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index) {
  // Prevents multiple threads from indexing the same chunk concurrently.
  const auto lock = std::lock_guard<std::shared_mutex>{_data_access_mutex};
  return _impl->insert(chunks_to_index, _column_id);
}

size_t PartialHashIndex::remove(const std::vector<ChunkID>& chunks_to_remove) {
  // Prevents multiple threads from removing the same chunk concurrently.
  const auto lock = std::lock_guard<std::shared_mutex>{_data_access_mutex};
  return _impl->remove(chunks_to_remove);
}

bool PartialHashIndex::indexed_null_values() const {
  return _impl->indexed_null_values();
}

PartialHashIndex::IteratorRange PartialHashIndex::_range_equals(const AllTypeVariant& value) const {
  return _impl->range_equals(value);
}

PartialHashIndex::IteratorRangePair PartialHashIndex::_range_not_equals(const AllTypeVariant& value) const {
  return _impl->range_not_equals(value);
}

PartialHashIndex::Iterator PartialHashIndex::_cbegin() const {
  return _impl->cbegin();
}

PartialHashIndex::Iterator PartialHashIndex::_cend() const {
  return _impl->cend();
}

PartialHashIndex::Iterator PartialHashIndex::_null_cbegin() const {
  return _impl->null_cbegin();
}

PartialHashIndex::Iterator PartialHashIndex::_null_cend() const {
  return _impl->null_cend();
}

size_t PartialHashIndex::estimate_memory_usage() const {
  auto bytes = size_t{0u};
  bytes += sizeof(_impl);
  bytes += sizeof(_column_id);
  bytes += sizeof(_data_access_mutex);
  bytes += _impl->estimate_memory_usage();
  return bytes;
}

bool PartialHashIndex::is_index_for(const ColumnID column_id) const {
  return column_id == _column_id;
}

tsl::sparse_set<ChunkID> PartialHashIndex::get_indexed_chunk_ids() const {
  return _impl->get_indexed_chunk_ids();
}

ColumnID PartialHashIndex::get_indexed_column_id() const {
  return _column_id;
}

}  // namespace hyrise
