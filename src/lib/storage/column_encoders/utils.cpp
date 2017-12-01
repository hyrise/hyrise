#include "utils.hpp"

#include <boost/hana/fold.hpp>

#include <memory>

#include "encoders.hpp"

#include "storage/base_value_column.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::unique_ptr<BaseColumnEncoder> create_encoder(ColumnEncodingType encoding_type) {
  Assert(encoding_type != ColumnEncodingType::Invalid, "Encoding type must be valid.");

  auto encoder = std::unique_ptr<BaseColumnEncoder>{};

  hana::fold(encoder_for_type, false, [&](auto match_found, auto pair) {
    if (!match_found && hana::first(pair) == encoding_type) {
      using EncoderType = typename decltype(+hana::second(pair))::type;
      encoder = std::make_unique<EncoderType>();
      return true;
    }

    return false;
  });

  return encoder;
}

std::shared_ptr<BaseColumn> encode_column(ColumnEncodingType encoding_type, DataType data_type,
                                          std::shared_ptr<BaseValueColumn> column) {
  auto encoder = create_encoder(encoding_type);
  return encoder->encode(data_type, column);
}

}  // namespace opossum
