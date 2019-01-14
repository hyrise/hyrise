#pragma once

#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp> // NEEDEDINCLUDE
#include <boost/hana/type.hpp> // NEEDEDINCLUDE
#include <cstdint>

namespace opossum {

namespace hana = boost::hana;

enum class SegmentIndexType : uint8_t { Invalid, GroupKey, CompositeGroupKey, AdaptiveRadixTree, BTree };

class GroupKeyIndex;
class CompositeGroupKeyIndex;
class AdaptiveRadixTreeIndex;
class BTreeIndex;

namespace detail {

constexpr auto segment_index_map =
    hana::make_map(hana::make_pair(hana::type_c<GroupKeyIndex>, SegmentIndexType::GroupKey),
                   hana::make_pair(hana::type_c<CompositeGroupKeyIndex>, SegmentIndexType::CompositeGroupKey),
                   hana::make_pair(hana::type_c<AdaptiveRadixTreeIndex>, SegmentIndexType::AdaptiveRadixTree),
                   hana::make_pair(hana::type_c<BTreeIndex>, SegmentIndexType::BTree));

}  // namespace detail

template <typename IndexType>
SegmentIndexType get_index_type_of() {
  return detail::segment_index_map[hana::type_c<IndexType>];
}

}  // namespace opossum
