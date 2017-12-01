#pragma once

#include <boost/hana/pair.hpp>
#include <boost/hana/tuple.hpp>

/**
 * Note: Include your column encoder file here!
 */
#include "dictionary_encoder.hpp"
#include "new_dictionary_encoder.hpp"

#include "storage/columns/column_encoding_type.hpp"

namespace opossum {

/**
 * Mapping of encoding types to encoders
 *
 * Note: Add your column encoder class here!
 */
constexpr auto encoder_for_type = hana::make_tuple(
  hana::make_pair(ColumnEncodingType::Dictionary, hana::type_c<DictionaryEncoder>),
  hana::make_pair(ColumnEncodingType::NewDictionary, hana::type_c<NewDictionaryEncoder>));

}  // namespace opossum
