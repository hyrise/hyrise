#include "segment_index_type.hpp"


namespace opossum {

std::string segment_index_type_to_string(SegmentIndexType type) {
  switch (type) {
    case SegmentIndexType::Invalid:
      return "Invalid";
    case SegmentIndexType::GroupKey:
      return "GroupKey";
    case SegmentIndexType::CompositeGroupKey:
      return "Composite Group Key";
    case SegmentIndexType::AdaptiveRadixTree:
      return "Adaptive Radix Tree";
    case SegmentIndexType::BTree:
      return "BTree";
    default:
      Fail("Invalid Index Type");
  }
}

}  // namespace opossum