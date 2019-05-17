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
 * @brief Encodes a segment by the given encoding specification (i.e., the encoding method
 * and -- when given and applicable -- the vector compression type)
 *
 * @return encoded segment if data type is supported, otherwise throws exception.
 */
std::shared_ptr<BaseEncodedSegment> encode_and_compress_segment(const std::shared_ptr<const BaseSegment>& segment,
                                                                const DataType data_type,
                                                                const SegmentEncodingSpec& encoding_spec);

VectorCompressionType parent_vector_compression_type(const CompressedVectorType compressed_vector_type);

}  // namespace opossum
