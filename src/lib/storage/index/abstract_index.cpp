#include "abstract_range_index.hpp"

#include <memory>
#include <vector>

#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/index/partial_hash/partial_hash_index.hpp"

namespace opossum {

template <typename PositionEntry>
size_t AbstractIndex<PositionEntry>::estimate_memory_consumption(SegmentIndexType type, ChunkOffset row_count,
                                                  ChunkOffset distinct_count, uint32_t value_bytes) {
  switch (type) {
    case SegmentIndexType::GroupKey:
      return GroupKeyIndex::estimate_memory_consumption(row_count, distinct_count, value_bytes);
    case SegmentIndexType::CompositeGroupKey:
      return CompositeGroupKeyIndex::estimate_memory_consumption(row_count, distinct_count, value_bytes);
    case SegmentIndexType::AdaptiveRadixTree:
      return AdaptiveRadixTreeIndex::estimate_memory_consumption(row_count, distinct_count, value_bytes);
    case SegmentIndexType::BTree:
      return BTreeIndex::estimate_memory_consumption(row_count, distinct_count, value_bytes);
    case SegmentIndexType::PartialHash:
      return PartialHashIndex::estimate_memory_consumption(row_count, distinct_count, value_bytes);
    case SegmentIndexType::Invalid:
      Fail("SegmentIndexType is invalid.");
  }
  Fail("GCC thinks this is reachable.");
}



}  // namespace opossum
