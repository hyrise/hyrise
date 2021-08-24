#pragma once

#include <cstdint>

#include <boost/hana/at_key.hpp>

#include "all_type_variant.hpp"

namespace opossum {

namespace hana = boost::hana;

enum class ChunkIndexType : uint8_t { Invalid, GroupKey, CompositeGroupKey, AdaptiveRadixTree, BTree };

class GroupKeyIndex;
class CompositeGroupKeyIndex;
class AdaptiveRadixTreeIndex;
class BTreeIndex;
class PartialHashIndex;

namespace detail {

constexpr auto chunk_index_map =
    hana::make_map(hana::make_pair(hana::type_c<GroupKeyIndex>, ChunkIndexType::GroupKey),
                   hana::make_pair(hana::type_c<CompositeGroupKeyIndex>, ChunkIndexType::CompositeGroupKey),
                   hana::make_pair(hana::type_c<AdaptiveRadixTreeIndex>, ChunkIndexType::AdaptiveRadixTree),
                   hana::make_pair(hana::type_c<BTreeIndex>, ChunkIndexType::BTree));

}  // namespace detail

template <typename IndexType>
ChunkIndexType get_chunk_index_type_of() {
  return detail::chunk_index_map[hana::type_c<IndexType>];
}

}  // namespace opossum
