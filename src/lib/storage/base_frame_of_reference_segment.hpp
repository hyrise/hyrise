#pragma once

#include <memory>

#include "base_encoded_segment.hpp"

namespace opossum {

class BaseCompressedVector;

/**
 * @brief Base class of FrameOfReferenceSegment<T> exposing type-independent interface
 */

class BaseFrameOfReferenceSegment : public BaseEncodedSegment {
 public:
  using BaseEncodedSegment::BaseEncodedSegment;

  EncodingType encoding_type() const override = 0;
  std::optional<CompressedVectorType> compressed_vector_type() const override = 0;
};

}  // namespace opossum
