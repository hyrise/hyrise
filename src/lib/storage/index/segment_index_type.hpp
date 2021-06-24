#pragma once

#include <cstdint>

#include <boost/hana/at_key.hpp>

#include "all_type_variant.hpp"

namespace opossum {

namespace hana = boost::hana;

enum class IndexType : uint8_t { Invalid, GroupKey, CompositeGroupKey, AdaptiveRadixTree, BTree, PartialHash };

class GroupKeyIndex;
class CompositeGroupKeyIndex;
class AdaptiveRadixTreeIndex;
class BTreeIndex;
class PartialHashIndex;

namespace detail {

constexpr auto segment_index_map =
    hana::make_map(hana::make_pair(hana::type_c<GroupKeyIndex>, IndexType::GroupKey),
                   hana::make_pair(hana::type_c<CompositeGroupKeyIndex>, IndexType::CompositeGroupKey),
                   hana::make_pair(hana::type_c<AdaptiveRadixTreeIndex>, IndexType::AdaptiveRadixTree),
                   hana::make_pair(hana::type_c<BTreeIndex>, IndexType::BTree),
                   hana::make_pair(hana::type_c<PartialHashIndex>, IndexType::PartialHash));

}  // namespace detail

template <typename Type>
IndexType get_index_type_of() {
  return detail::segment_index_map[hana::type_c<Type>];
}

}  // namespace opossum
