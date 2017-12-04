#pragma once

#include <boost/hana/pair.hpp>
#include <boost/hana/map.hpp>

// Include your encoded column file here!
#include "storage/dictionary_column.hpp"
#include "new_dictionary_column.hpp"

#include "column_encoding_type.hpp"

#include "utils/enum_constant.hpp"

namespace opossum {

/**
 * @brief Mapping of encoding types to columns
 *
 * Unfortunately, template classes cannot be stored in hana data structures.
 * For this reason, every encoded column template class needs an info struct
 * containing using declaration to the actual template class.
 *
 * Note: Add your encoded column class here!
 */
constexpr auto encoded_column_info_for_type = hana::make_map(
  hana::make_pair(enum_c<EncodingType::Dictionary>, hana::type_c<DictionaryColumnInfo>),
  hana::make_pair(enum_c<EncodingType::NewDictionary>, hana::type_c<NewDictionaryColumnInfo>));

}  // namespace opossum
