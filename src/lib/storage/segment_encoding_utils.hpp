#pragma once

#include <memory>

#include "storage/abstract_segment.hpp"
#include "storage/encoding_type.hpp"
#include "storage/vector_compression/vector_compression.hpp"

namespace hyrise {

class AbstractEncodedSegment;
class BaseSegmentEncoder;
class BaseValueSegment;

/**
 * @brief Creates an encoder by encoding type
 */
std::unique_ptr<BaseSegmentEncoder> create_encoder(EncodingType encoding_type);

/**
 * @return the segment encoding spec for the given segment.
 */
SegmentEncodingSpec get_segment_encoding_spec(const std::shared_ptr<const AbstractSegment>& segment);

/**
 * @brief Returns the vector compression type for a given compressed vector type.
 *
 * For the difference of the two, please take a look at compressed_vector_type.hpp.
 */
VectorCompressionType parent_vector_compression_type(const CompressedVectorType compressed_vector_type);

SegmentEncodingSpec auto_select_segment_encoding_spec(const DataType& type, const bool is_unique = false);

}  // namespace hyrise
