#pragma once

#include <cstdint>
#include <utility>

#include <boost/hana/at_key.hpp>
#include <boost/hana/map.hpp>

#include "all_type_variant.hpp"

namespace hyrise {

namespace hana = boost::hana;

enum class ChunkIndexType : uint8_t { GroupKey, CompositeGroupKey, AdaptiveRadixTree };

class GroupKeyIndex;
class CompositeGroupKeyIndex;
class AdaptiveRadixTreeIndex;
class PartialHashIndex;

namespace detail {

constexpr auto chunk_index_map =
    hana::make_map(hana::make_pair(hana::type_c<GroupKeyIndex>, ChunkIndexType::GroupKey),
                   hana::make_pair(hana::type_c<CompositeGroupKeyIndex>, ChunkIndexType::CompositeGroupKey),
                   hana::make_pair(hana::type_c<AdaptiveRadixTreeIndex>, ChunkIndexType::AdaptiveRadixTree));

}  // namespace detail

template <typename IndexType>
ChunkIndexType get_chunk_index_type_of() {
  return detail::chunk_index_map[hana::type_c<IndexType>];
}

}  // namespace hyrise
