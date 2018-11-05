#include "base_index.hpp"

#include <memory>
#include <vector>

#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"

namespace opossum {

size_t BaseIndex::estimate_memory_consumption(SegmentIndexType type, ChunkOffset row_count, ChunkOffset distinct_count,
                                              uint32_t value_bytes) {
  switch (type) {
    case SegmentIndexType::GroupKey:
      return GroupKeyIndex::estimate_memory_consumption(row_count, distinct_count, value_bytes);
    case SegmentIndexType::CompositeGroupKey:
      return CompositeGroupKeyIndex::estimate_memory_consumption(row_count, distinct_count, value_bytes);
    case SegmentIndexType::AdaptiveRadixTree:
      return AdaptiveRadixTreeIndex::estimate_memory_consumption(row_count, distinct_count, value_bytes);
    case SegmentIndexType::BTree:
      return BTreeIndex::estimate_memory_consumption(row_count, distinct_count, value_bytes);
    default:
      Fail("estimate_memory_consumption() is not implemented for the given index type");
  }
}

BaseIndex::BaseIndex(const SegmentIndexType type) : _type{type} {}

bool BaseIndex::is_index_for(const std::vector<std::shared_ptr<const BaseSegment>>& segments) const {
  auto indexed_segments = _get_indexed_segments();
  if (segments.size() > indexed_segments.size()) return false;
  if (segments.empty()) return false;

  for (size_t i = 0; i < segments.size(); ++i) {
    if (segments[i] != indexed_segments[i]) return false;
  }
  return true;
}

BaseIndex::Iterator BaseIndex::lower_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert((_get_indexed_segments().size() >= values.size()),
              "BaseIndex: The number of queried segments has to be less or equal to the number of indexed segments.");

  return _lower_bound(values);
}

BaseIndex::Iterator BaseIndex::upper_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert((_get_indexed_segments().size() >= values.size()),
              "BaseIndex: The number of queried segments has to be less or equal to the number of indexed segments.");

  return _upper_bound(values);
}

BaseIndex::Iterator BaseIndex::cbegin() const { return _cbegin(); }

BaseIndex::Iterator BaseIndex::cend() const { return _cend(); }

SegmentIndexType BaseIndex::type() const { return _type; }

size_t BaseIndex::memory_consumption() const { return _memory_consumption(); }

}  // namespace opossum
