#pragma once

#include <boost/hana/at_key.hpp>

#include <cstdint>

#include "all_type_variant.hpp"

namespace opossum {

namespace hana = boost::hana;

enum class ColumnIndexType : uint8_t { Invalid, GroupKey, CompositeGroupKey, AdaptiveRadixTree };

class GroupKeyIndex;
class CompositeGroupKeyIndex;
class AdaptiveRadixTreeIndex;

namespace detail {

constexpr auto column_index_map =
    hana::make_map(hana::make_pair(hana::type_c<GroupKeyIndex>, ColumnIndexType::GroupKey),
                   hana::make_pair(hana::type_c<CompositeGroupKeyIndex>, ColumnIndexType::CompositeGroupKey),
                   hana::make_pair(hana::type_c<AdaptiveRadixTreeIndex>, ColumnIndexType::AdaptiveRadixTree));

}  // namespace detail

template <typename IndexType>
ColumnIndexType get_index_type_of() {
  return detail::column_index_map[hana::type_c<IndexType>];
}

}  // namespace opossum
