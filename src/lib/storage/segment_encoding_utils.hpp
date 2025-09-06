#pragma once

#include <memory>
#include <vector>

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

ChunkEncodingSpec auto_select_chunk_encoding_spec(const std::vector<DataType>& types,
                                                  const std::vector<bool>& chunk_values_are_unique);
SegmentEncodingSpec auto_select_segment_encoding_spec(const DataType type,
                                                      const bool segment_values_are_unique = false);

}  // namespace hyrise
