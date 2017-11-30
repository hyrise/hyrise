#pragma once

#include <boost/hana/fold.hpp>

#include <memory>

#include "encoders.hpp"

#include "storage/base_value_column.hpp"
#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename Functor>
void with_encoder(ColumnEncodingType encoding_type, const Functor& functor) {
  hana::fold(encoder_for_type, false, [&](auto match_found, auto pair) {
    if (!match_found && hana::first(pair) == encoding_type) {
      using EncoderType = typename decltype(+hana::second(pair))::type;
      functor(EncoderType{});
      return true;
    }

    return false;
  });
}

std::shared_ptr<BaseColumn> encode_column(ColumnEncodingType encoding_type, DataType data_type,
                                          std::shared_ptr<BaseValueColumn> column) {
  auto encoded_column = std::shared_ptr<BaseColumn>{};

  resolve_data_type(data_type, [&](auto data_type_obj) {
    with_encoder(encoding_type, [&](auto encoder) {
      if constexpr (encoder.supports(data_type_obj)) {
        encoded_column = encoder.encode(data_type_obj, value_column);
      } else {
        Fail("Data type not supported by encoder.");
      }
    });
  });

  return encoded_column;
}

}  // namespace
