#include <utility>

#include <boost/hana/at_key.hpp>
#include <boost/hana/map.hpp>

#include "all_type_variant.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"

namespace hyrise {
namespace detail {

namespace hana = boost::hana;

constexpr auto CHUNK_INDEX_MAP =
    hana::make_map(hana::make_pair(hana::type_c<GroupKeyIndex>, ChunkIndexType::GroupKey),
                   hana::make_pair(hana::type_c<CompositeGroupKeyIndex>, ChunkIndexType::CompositeGroupKey),
                   hana::make_pair(hana::type_c<AdaptiveRadixTreeIndex>, ChunkIndexType::AdaptiveRadixTree));

}  // namespace detail

template <typename IndexType>
ChunkIndexType get_chunk_index_type_of() {
  return detail::CHUNK_INDEX_MAP[hana::type_c<IndexType>];
}

}  // namespace hyrise
