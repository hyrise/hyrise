#include "column_encoding_utils.hpp"

#include <map>
#include <memory>

#include "storage/dictionary_column/dictionary_encoder.hpp"
#include "storage/frame_of_reference/frame_of_reference_encoder.hpp"
#include "storage/run_length_column/run_length_encoder.hpp"

#include "storage/base_value_column.hpp"
#include "utils/assert.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

namespace {

/**
 * @brief Mapping of encoding types to encoders
 *
 * Add your column encoder here!
 */
const auto encoder_for_type = std::map<EncodingType, std::shared_ptr<BaseColumnEncoder>>{
    {EncodingType::Dictionary, std::make_shared<DictionaryEncoder<EncodingType::Dictionary>>()},
    {EncodingType::RunLength, std::make_shared<RunLengthEncoder>()},
    {EncodingType::FixedStringDictionary, std::make_shared<DictionaryEncoder<EncodingType::FixedStringDictionary>>()},
    {EncodingType::FrameOfReference, std::make_shared<FrameOfReferenceEncoder>()}};

}  // namespace

std::unique_ptr<BaseColumnEncoder> create_encoder(EncodingType encoding_type) {
  Assert(encoding_type != EncodingType::Unencoded, "Encoding type must not be Unencoded`.");

  auto it = encoder_for_type.find(encoding_type);
  Assert(it != encoder_for_type.cend(), "All encoding types must be in encoder_for_type.");

  const auto& encoder = it->second;
  return encoder->create_new();
}

std::shared_ptr<BaseEncodedColumn> encode_column(EncodingType encoding_type, DataType data_type,
                                                 const std::shared_ptr<const BaseValueColumn>& column,
                                                 std::optional<VectorCompressionType> zero_suppression_type) {
  auto encoder = create_encoder(encoding_type);

  if (zero_suppression_type.has_value()) {
    encoder->set_vector_compression(*zero_suppression_type);
  }

  return encoder->encode(column, data_type);
}

}  // namespace opossum
