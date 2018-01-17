#include "column_encoding_utils.hpp"

#include <boost/hana/fold.hpp>
#include <boost/hana/pair.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/value.hpp>

#include <memory>

#include "storage/deprecated_dictionary_column/deprecated_dictionary_encoder.hpp"
#include "storage/dictionary_column/dictionary_encoder.hpp"
#include "storage/run_length_column/run_length_encoder.hpp"
#include "storage/base_value_column.hpp"
#include "utils/enum_constant.hpp"
#include "utils/assert.hpp"

namespace opossum {

namespace {

/**
 * @brief Mapping of encoding types to encoders
 */
constexpr auto encoder_for_type =
    hana::make_tuple(hana::make_pair(enum_c<EncodingType, EncodingType::DeprecatedDictionary>,
                                     hana::type_c<DeprecatedDictionaryEncoder>),
                     hana::make_pair(enum_c<EncodingType, EncodingType::Dictionary>, hana::type_c<DictionaryEncoder>),
                     hana::make_pair(enum_c<EncodingType, EncodingType::RunLength>, hana::type_c<RunLengthEncoder>));

}  // namespace

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

std::shared_ptr<BaseEncodedColumn> encode_column(EncodingType encoding_type, DataType data_type,
                                                 std::shared_ptr<const BaseValueColumn> column,
                                                 std::optional<ZsType> zero_suppression_type) {
  auto encoder = create_encoder(encoding_type);

  if (zero_suppression_type.has_value()) {
    encoder->set_zs_type(zero_suppression_type.value());
  }

  return encoder->encode(data_type, column);
}

}  // namespace opossum
