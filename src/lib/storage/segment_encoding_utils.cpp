#include "segment_encoding_utils.hpp"

#include <map>
#include <memory>

#include "storage/dictionary_segment/dictionary_encoder.hpp"
#include "storage/frame_of_reference_segment/frame_of_reference_encoder.hpp"
#include "storage/lz4_segment/lz4_encoder.hpp"
#include "storage/run_length_segment/run_length_encoder.hpp"

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

std::shared_ptr<BaseEncodedSegment> encode_and_compress_segment(const std::shared_ptr<const BaseSegment>& segment,
                                                                const DataType data_type,
                                                                const SegmentEncodingSpec& encoding_spec) {
  auto encoder = create_encoder(encoding_spec.encoding_type);

  if (encoding_spec.vector_compression_type) {
    encoder->set_vector_compression(*encoding_spec.vector_compression_type);
  }

  return encoder->encode(segment, data_type);
}

SegmentEncodingSpec get_segment_encoding_spec(const std::shared_ptr<const BaseSegment>& segment) {
  SegmentEncodingSpec result;
  resolve_data_type(segment->data_type(), [&](const auto type) {
    using ColumnDataType = typename decltype(type)::type;

    if (const auto reference_segment = std::dynamic_pointer_cast<const ReferenceSegment>(segment)) {
      Fail("Reference segments cannot be encoded.");
    }

    const auto unencoded_segment = std::dynamic_pointer_cast<const ValueSegment<ColumnDataType>>(segment);
    if (unencoded_segment) {
      result = SegmentEncodingSpec{EncodingType::Unencoded};
      return;
    }

    const auto encoded_segment = std::dynamic_pointer_cast<const BaseEncodedSegment>(segment);
    if (encoded_segment) {
      std::optional<VectorCompressionType> vector_compression = std::nullopt;
      if (!encoded_segment->compressed_vector_type()) {
        vector_compression = parent_vector_compression_type(*encoded_segment->compressed_vector_type());
      }
      result = SegmentEncodingSpec{encoded_segment->encoding_type(), vector_compression};
      return;
    }

    Fail("Unexpected segment encoding found.");
  });
  return result;
}

VectorCompressionType parent_vector_compression_type(const CompressedVectorType compressed_vector_type) {
  switch (compressed_vector_type) {
    case CompressedVectorType::FixedSize4ByteAligned:
    case CompressedVectorType::FixedSize2ByteAligned:
    case CompressedVectorType::FixedSize1ByteAligned:
      return VectorCompressionType::FixedSizeByteAligned;
      break;
    case CompressedVectorType::SimdBp128:
      return VectorCompressionType::SimdBp128;
  }
  Fail("Invalid enum value");
}

}  // namespace opossum
