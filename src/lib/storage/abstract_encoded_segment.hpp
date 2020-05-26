#pragma once

#include "storage/abstract_segment.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

enum class CompressedVectorType : uint8_t;

/**
 * @brief Base class of all encoded segments
 *
 * Since encoded segments are immutable, all member variables
 * of sub-classes should be declared const.
 */
class AbstractEncodedSegment : public AbstractSegment {
 public:
  using AbstractSegment::AbstractSegment;

  virtual EncodingType encoding_type() const = 0;

  /**
   * An encoded segment may use a compressed vector to reduce its memory footprint.
   * Returns the vector’s type if it does, else std::nullopt
   */
  virtual std::optional<CompressedVectorType> compressed_vector_type() const = 0;
};

}  // namespace opossum
