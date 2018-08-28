#pragma once

#include <memory>
#include <optional>

#include "all_type_variant.hpp"
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
 * @brief Encodes a value segment by the given encoding method
 *
 * @return encoded segment if data type is supported else throws exception
 */
std::shared_ptr<BaseEncodedSegment> encode_segment(EncodingType encoding_type, DataType data_type,
                                                   const std::shared_ptr<const BaseValueSegment>& segment,
                                                   std::optional<VectorCompressionType> zero_suppression_type = {});

}  // namespace opossum
