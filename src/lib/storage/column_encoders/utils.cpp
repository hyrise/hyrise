#include "utils.hpp"

#include <boost/hana/fold.hpp>
#include <boost/hana/value.hpp>

#include <memory>

#include "encoders.hpp"

#include "storage/base_value_column.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::unique_ptr<BaseColumnEncoder> create_encoder(EncodingType encoding_type) {
  Assert(encoding_type != EncodingType::Invalid, "Encoding type must be valid.");

  auto encoder = std::unique_ptr<BaseColumnEncoder>{};

  hana::fold(encoder_for_type, false, [&](auto match_found, auto pair) {
    const auto encoding_type_c = hana::first(pair);
    const auto encoder_t = hana::second(pair);

    if (!match_found && hana::value(encoding_type_c) == encoding_type) {
      using EncoderType = typename decltype(encoder_t)::type;
      encoder = std::make_unique<EncoderType>();
      return true;
    }

    return match_found;
  });

  return encoder;
}

std::shared_ptr<BaseColumn> encode_column(EncodingType encoding_type, DataType data_type,
                                          std::shared_ptr<BaseValueColumn> column) {
  auto encoder = create_encoder(encoding_type);
  return encoder->encode(data_type, column);
}

}  // namespace opossum
