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
 * @return the segment encoding spec for the given segment.
 */
SegmentEncodingSpec get_segment_encoding_spec(const std::shared_ptr<const BaseSegment>& segment);

/**
 * @brief Returns the vector compression type for a given compressed vector type.
 *
 * For the difference of the two, please take a look at compressed_vector_type.hpp.
 */
VectorCompressionType parent_vector_compression_type(const CompressedVectorType compressed_vector_type);

}  // namespace opossum
