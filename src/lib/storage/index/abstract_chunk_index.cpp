#include "abstract_chunk_index.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/chunk_index_type.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

size_t AbstractChunkIndex::estimate_memory_consumption(ChunkIndexType type, ChunkOffset row_count,
                                                       ChunkOffset distinct_count, uint32_t value_bytes) {
  switch (type) {
    case ChunkIndexType::GroupKey:
      return GroupKeyIndex::estimate_memory_consumption(row_count, distinct_count, value_bytes);
    case ChunkIndexType::CompositeGroupKey:
      return CompositeGroupKeyIndex::estimate_memory_consumption(row_count, distinct_count, value_bytes);
    case ChunkIndexType::AdaptiveRadixTree:
      return AdaptiveRadixTreeIndex::estimate_memory_consumption(row_count, distinct_count, value_bytes);
  }
  Fail("GCC thinks this is reachable.");
}

AbstractChunkIndex::AbstractChunkIndex(const ChunkIndexType type) : _type{type} {}

bool AbstractChunkIndex::is_index_for(const std::vector<std::shared_ptr<const AbstractSegment>>& segments) const {
  const auto& indexed_segments = _get_indexed_segments();
  if (segments.size() > indexed_segments.size() || segments.empty()) {
    return false;
  }

  for (auto segment_idx = size_t{0}; segment_idx < segments.size(); ++segment_idx) {
    if (segments[segment_idx] != indexed_segments[segment_idx]) {
      return false;
    }
  }
  return true;
}

AbstractChunkIndex::Iterator AbstractChunkIndex::lower_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert(
      _get_indexed_segments().size() >= values.size(),
      "AbstractChunkIndex: The number of queried segments has to be less or equal to the number of indexed segments.");

  return _lower_bound(values);
}

AbstractChunkIndex::Iterator AbstractChunkIndex::upper_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert(
      _get_indexed_segments().size() >= values.size(),
      "AbstractChunkIndex: The number of queried segments has to be less or equal to the number of indexed segments.");

  return _upper_bound(values);
}

AbstractChunkIndex::Iterator AbstractChunkIndex::cbegin() const {
  return _cbegin();
}

AbstractChunkIndex::Iterator AbstractChunkIndex::cend() const {
  return _cend();
}

AbstractChunkIndex::Iterator AbstractChunkIndex::null_cbegin() const {
  return _null_positions.cbegin();
}

AbstractChunkIndex::Iterator AbstractChunkIndex::null_cend() const {
  return _null_positions.cend();
}

ChunkIndexType AbstractChunkIndex::type() const {
  return _type;
}

size_t AbstractChunkIndex::memory_consumption() const {
  size_t bytes{0u};
  bytes += _memory_consumption();
  bytes += sizeof(std::vector<ChunkOffset>);  // _null_positions
  bytes += sizeof(ChunkOffset) * _null_positions.capacity();
  bytes += sizeof(_type);
  return bytes;
}

}  // namespace hyrise
