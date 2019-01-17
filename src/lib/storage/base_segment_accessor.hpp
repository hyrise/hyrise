#pragma once

#include "resolve_type.hpp"

namespace opossum {

/**
 * This is the base class for all SegmentAccessor types.
 * It provides the common interface to access individual values of a segment.
 */
template <typename T>
class BaseSegmentAccessor {
 public:
  BaseSegmentAccessor() = default;
  BaseSegmentAccessor(const BaseSegmentAccessor&) = default;
  BaseSegmentAccessor(BaseSegmentAccessor&&) = default;

  virtual ~BaseSegmentAccessor() {}

  virtual const std::optional<T> access(ChunkOffset offset) const = 0;
};

}  // namespace opossum
