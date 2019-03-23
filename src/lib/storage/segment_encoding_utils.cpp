#include "segment_encoding_utils.hpp"

#include <map>
#include <memory>

#include "storage/dictionary_segment/dictionary_encoder.hpp"
#include "storage/frame_of_reference/frame_of_reference_encoder.hpp"
#include "storage/lz4/lz4_encoder.hpp"
#include "storage/run_length_segment/run_length_encoder.hpp"

#include "storage/base_value_segment.hpp"
#include "utils/assert.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

namespace {

/**
 * @brief Mapping of encoding types to encoders
 *
 * Add your segment encoder here!
 */
const auto encoder_for_type = std::map<EncodingType, std::shared_ptr<BaseSegmentEncoder>>{
    {EncodingType::Dictionary, std::make_shared<DictionaryEncoder<EncodingType::Dictionary>>()},
    {EncodingType::RunLength, std::make_shared<RunLengthEncoder>()},
    {EncodingType::FixedStringDictionary, std::make_shared<DictionaryEncoder<EncodingType::FixedStringDictionary>>()},
    {EncodingType::FrameOfReference, std::make_shared<FrameOfReferenceEncoder>()},
    {EncodingType::LZ4, std::make_shared<LZ4Encoder>()}};

}  // namespace

std::unique_ptr<BaseSegmentEncoder> create_encoder(EncodingType encoding_type) {
  Assert(encoding_type != EncodingType::Unencoded, "Encoding type must not be Unencoded`.");

  auto it = encoder_for_type.find(encoding_type);
  Assert(it != encoder_for_type.cend(), "All encoding types must be in encoder_for_type.");

  const auto& encoder = it->second;
  return encoder->create_new();
}

std::shared_ptr<BaseEncodedSegment> encode_segment(EncodingType encoding_type, DataType data_type,
                                                   const std::shared_ptr<const BaseValueSegment>& segment,
                                                   std::optional<VectorCompressionType> zero_suppression_type) {
  auto encoder = create_encoder(encoding_type);

  if (zero_suppression_type.has_value()) {
    encoder->set_vector_compression(*zero_suppression_type);
  }

  return encoder->encode(segment, data_type);
}

}  // namespace opossum
