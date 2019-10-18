#pragma once

#include <memory>

namespace opossum {

class BaseSegment;
class BaseDictionarySegment;
class BaseEncodedSegment;
class BaseValueSegment;
class ReferenceSegment;
class BaseRunLengthSegment;

class SegmentVisitorContext {};

// In cases where an operator has to operate on different segment types, we use the visitor pattern.
// By inheriting from AbstractSegmentVisitor, an AbstractOperator(Impl) can implement handle methods for all segment
// types.
class AbstractSegmentVisitor {
 public:
  virtual ~AbstractSegmentVisitor() = default;
  virtual void handle_segment(const BaseValueSegment& segment, std::shared_ptr<SegmentVisitorContext> context) = 0;
  virtual void handle_segment(const BaseDictionarySegment& segment, std::shared_ptr<SegmentVisitorContext> context) = 0;
  virtual void handle_segment(const ReferenceSegment& segment, std::shared_ptr<SegmentVisitorContext> context) = 0;
  virtual void handle_segment(const BaseEncodedSegment& segment, std::shared_ptr<SegmentVisitorContext> context) = 0;
  virtual void handle_segment(const BaseRunLengthSegment& segment, std::shared_ptr<SegmentVisitorContext> context) = 0;
};

}  // namespace opossum
