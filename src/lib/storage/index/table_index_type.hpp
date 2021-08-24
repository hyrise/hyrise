#pragma once

#include <cstdint>

#include <boost/hana/at_key.hpp>

#include "all_type_variant.hpp"

namespace opossum {

namespace hana = boost::hana;

enum class TableIndexType : uint8_t { Invalid, PartialHash };

class PartialHashIndex;

namespace detail {

constexpr auto table_index_map =
    hana::make_map(hana::make_pair(hana::type_c<PartialHashIndex>, TableIndexType::PartialHash));

}  // namespace detail

template <typename IndexType>
TableIndexType get_table_index_type_of() {
  return detail::table_index_map[hana::type_c<IndexType>];
}

}  // namespace opossum
