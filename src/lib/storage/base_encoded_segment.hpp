#pragma once

#include "storage/base_segment.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

enum class CompressedVectorType : uint8_t;

/**
 * @brief Base class of all encoded segments
 *
 * Since encoded segments are immutable, all member variables
 * of sub-classes should be declared const.
 */
class BaseEncodedSegment : public BaseSegment {
 public:
  using BaseSegment::BaseSegment;

  virtual EncodingType encoding_type() const = 0;

  /**
   * An encoded segment may use a compressed vector to reduce its memory footprint.
   * Returns the vector’s type if it does, else CompressedVectorType::Invalid
   */
  virtual CompressedVectorType compressed_vector_type() const;
};

}  // namespace opossum
