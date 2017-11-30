#pragma once

#include <boost/hana/pair.hpp>
#include <boost/hana/tuple.hpp>

#include "dictionary_encoder.hpp"

#include "columns/column_encoding_type.hpp"

namespace opossum {

constexpr encoder_for_type = hana::make_tuple(
  hana::make_pair(ColumnEncodingType::Dictionary, hana::type_c<DictionaryEncoder>));

}  // namespace opossum
