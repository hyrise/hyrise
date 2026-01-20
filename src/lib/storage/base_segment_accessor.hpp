#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include "resolve_type.hpp"
#include "types.hpp"

namespace hyrise {

class BaseSegmentAccessor {
 public:
  BaseSegmentAccessor() = default;
  BaseSegmentAccessor(const BaseSegmentAccessor&) = default;
  BaseSegmentAccessor(BaseSegmentAccessor&&) = default;
  BaseSegmentAccessor& operator=(const BaseSegmentAccessor&) = default;
  BaseSegmentAccessor& operator=(BaseSegmentAccessor&&) = default;

  virtual ~BaseSegmentAccessor() = default;
};

/**
 * This is the base class for all SegmentAccessor types.
 * It provides the common interface to access individual values of a segment.
 */
template <typename T>
class AbstractSegmentAccessor : public BaseSegmentAccessor {
 public:
  virtual std::optional<T> access(ChunkOffset offset) const = 0;
};

}  // namespace hyrise
