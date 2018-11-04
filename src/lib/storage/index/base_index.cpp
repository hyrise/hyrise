#include "base_index.hpp"

#include <memory>
#include <vector>

namespace opossum {

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

}  // namespace opossum
