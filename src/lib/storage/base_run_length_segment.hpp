#pragma once

#include <memory>

#include "base_encoded_segment.hpp"

namespace opossum {

class BaseCompressedVector;

/**
 * @brief Base class of RunLengthSegment<T> exposing type-independent interface
 */

class BaseRunLengthSegment : public BaseEncodedSegment {
 public:
  using BaseEncodedSegment::BaseEncodedSegment;

  EncodingType encoding_type() const override = 0;
};

}  // namespace opossum
