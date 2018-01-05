#pragma once

#include <boost/hana/pair.hpp>
#include <boost/hana/tuple.hpp>

// Include your column encoder file here!
#include "deprecated_dictionary_encoder.hpp"
#include "dictionary_encoder.hpp"
#include "run_length_encoder.hpp"

#include "storage/encoded_columns/column_encoding_type.hpp"

#include "utils/enum_constant.hpp"

namespace opossum {

/**
 * @brief Mapping of encoding types to encoders
 *
 * Note: Add your column encoder class here!
 */
constexpr auto encoder_for_type =
    hana::make_tuple(hana::make_pair(enum_c<EncodingType, EncodingType::DeprecatedDictionary>,
                                     hana::type_c<DeprecatedDictionaryEncoder>),
                     hana::make_pair(enum_c<EncodingType, EncodingType::Dictionary>, hana::type_c<DictionaryEncoder>),
                     hana::make_pair(enum_c<EncodingType, EncodingType::RunLength>, hana::type_c<RunLengthEncoder>));

}  // namespace opossum
