#pragma once

#include <boost/hana/pair.hpp>
#include <boost/hana/tuple.hpp>

/**
 * Note: Include your encoded column file here!
 */
#include "storage/dictionary_column.hpp"
#include "new_dictionary_column.hpp"

#include "column_encoding_type.hpp"

namespace opossum {

/**
 * Mapping of encoding types to columns
 *
 * Note: Add your encoded column class here!
 */
constexpr auto encoded_column_for_type = hana::make_tuple(
  hana::make_pair(ColumnEncodingType::Dictionary, hana::type_c<DictionaryColumn>),
  hana::make_pair(ColumnEncodingType::NewDictionary, hana::type_c<NewDictionaryColumn>));

}  // namespace opossum
