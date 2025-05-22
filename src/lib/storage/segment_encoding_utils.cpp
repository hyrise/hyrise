#include "segment_encoding_utils.hpp"

#include <map>
#include <memory>
#include <optional>

#include "all_type_variant.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/dictionary_segment/dictionary_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/frame_of_reference_segment/frame_of_reference_encoder.hpp"
#include "storage/lz4_segment/lz4_encoder.hpp"
#include "storage/reference_segment.hpp"
#include "storage/run_length_segment/run_length_encoder.hpp"
#include "storage/vector_compression/compressed_vector_type.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "utils/assert.hpp"

namespace hyrise {

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

  auto iter = encoder_for_type.find(encoding_type);
  Assert(iter != encoder_for_type.cend(), "All encoding types must be in encoder_for_type.");

  const auto& encoder = iter->second;
  return encoder->create_new();
}

SegmentEncodingSpec get_segment_encoding_spec(const std::shared_ptr<const AbstractSegment>& segment) {
  Assert(!std::dynamic_pointer_cast<const ReferenceSegment>(segment), "Reference segments cannot be encoded.");

  if (std::dynamic_pointer_cast<const BaseValueSegment>(segment)) {
    return SegmentEncodingSpec{EncodingType::Unencoded};
  }

  if (const auto encoded_segment = std::dynamic_pointer_cast<const AbstractEncodedSegment>(segment)) {
    std::optional<VectorCompressionType> vector_compression;
    if (encoded_segment->compressed_vector_type()) {
      vector_compression = parent_vector_compression_type(*encoded_segment->compressed_vector_type());
    }
    return SegmentEncodingSpec{encoded_segment->encoding_type(), vector_compression};
  }

  Fail("Unexpected segment encoding found.");
}

VectorCompressionType parent_vector_compression_type(const CompressedVectorType compressed_vector_type) {
  switch (compressed_vector_type) {
    case CompressedVectorType::FixedWidthInteger4Byte:
    case CompressedVectorType::FixedWidthInteger2Byte:
    case CompressedVectorType::FixedWidthInteger1Byte:
      return VectorCompressionType::FixedWidthInteger;
      break;
    case CompressedVectorType::BitPacking:
      return VectorCompressionType::BitPacking;
  }
  Fail("Invalid enum value.");
}

ChunkEncodingSpec auto_select_chunk_encoding_spec(const std::vector<DataType>& types,
                                                  const std::vector<bool>& column_values_are_unique) {
  DebugAssert(types.size() == column_values_are_unique.size(), "The length of the two passed vectors has to match");

  const auto size = types.size();
  auto chunk_encoding_spec = ChunkEncodingSpec{};
  for (auto column_id = ColumnID{0}; column_id < size; ++column_id) {
    chunk_encoding_spec.push_back(
        auto_select_segment_encoding_spec(types[column_id], column_values_are_unique[column_id]));
  }
  return chunk_encoding_spec;
}

SegmentEncodingSpec auto_select_segment_encoding_spec(const DataType& type, const bool segment_values_are_unique) {
  switch (type) {
    case DataType::Int:
      return SegmentEncodingSpec{EncodingType::FrameOfReference};
    case DataType::String:
      return SegmentEncodingSpec{EncodingType::FixedStringDictionary};
    case DataType::Long:
    case DataType::Double:
    case DataType::Float:
      if (segment_values_are_unique) {
        return SegmentEncodingSpec{EncodingType::Unencoded};
      } else {
        return SegmentEncodingSpec{EncodingType::Dictionary};
      }
    default:
      Fail("Unknown DataType when trying to select encoding for column.");
  }
}

}  // namespace hyrise
