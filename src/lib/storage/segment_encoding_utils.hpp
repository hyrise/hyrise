#pragma once

#include <memory>
#include <optional>

#include "all_type_variant.hpp"
#include "storage/base_segment.hpp"
#include "storage/encoding_type.hpp"
#include "storage/vector_compression/vector_compression.hpp"

namespace opossum {

class BaseEncodedSegment;
class BaseSegmentEncoder;
class BaseValueSegment;

/**
 * @brief Creates an encoder by encoding type
 */
std::unique_ptr<BaseSegmentEncoder> create_encoder(EncodingType encoding_type);

/**
 * @brief Encodes a segment by the given encoding specification (i.e., the encoding method and -- when given and
 * applicable -- the vector compression type). In general, do not use this method but rather
 * ChunkEncoder::encode_segment() which supports reencoding to ValueSegments and avoids reencoding with
 * SegmentEncodingSpec already in place.
 *
 * @return encoded segment if data type is supported, otherwise throws exception.
 */
std::shared_ptr<BaseEncodedSegment> encode_and_compress_segment(const std::shared_ptr<const BaseSegment>& segment,
                                                                const DataType data_type,
                                                                const SegmentEncodingSpec& encoding_spec);

/**
 * @return the segment encoding spec for thes given segment.
 */
SegmentEncodingSpec get_segment_encoding_spec(const std::shared_ptr<const BaseSegment>& segment);

/**
 * @brief Returns the vector compression type for a given compressed vector type.
 *
 * For the difference of the two, please take a look at compressed_vector_type.hpp.
 */
VectorCompressionType parent_vector_compression_type(const CompressedVectorType compressed_vector_type);

}  // namespace opossum
