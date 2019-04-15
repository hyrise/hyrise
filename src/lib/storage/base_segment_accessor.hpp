#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

class BaseSegmentAccessor {
 public:
  BaseSegmentAccessor() = default;
  BaseSegmentAccessor(const BaseSegmentAccessor&) = default;
  BaseSegmentAccessor(BaseSegmentAccessor&&) = default;

  virtual ~BaseSegmentAccessor() {}
};

/**
 * This is the base class for all SegmentAccessor types.
 * It provides the common interface to access individual values of a segment.
 */
template <typename T>
class AbstractSegmentAccessor : public BaseSegmentAccessor {
 public:
  // This is a rare exception to the "no raw pointers rule", see segment_accessors.hpp.
  virtual const T* access(ChunkOffset offset) const = 0;
};

}  // namespace opossum
