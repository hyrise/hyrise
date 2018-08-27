#pragma once

#include <memory>

namespace opossum {

class BaseSegment;
class BaseDictionarySegment;
class BaseEncodedSegment;
class BaseValueSegment;
class ReferenceSegment;

class SegmentVisitorContext {};

// In cases where an operator has to operate on different column types, we use the visitor pattern.
// By inheriting from AbstractColumnVisitor, an AbstractOperator(Impl) can implement handle methods for all column
// types.
class AbstractColumnVisitor {
 public:
  virtual ~AbstractColumnVisitor() = default;
  virtual void handle_segment(const BaseValueSegment& column, std::shared_ptr<SegmentVisitorContext> context) = 0;
  virtual void handle_segment(const BaseDictionarySegment& column, std::shared_ptr<SegmentVisitorContext> context) = 0;
  virtual void handle_segment(const ReferenceSegment& column, std::shared_ptr<SegmentVisitorContext> context) = 0;
  virtual void handle_segment(const BaseEncodedSegment& column, std::shared_ptr<SegmentVisitorContext> context) = 0;
};

}  // namespace opossum
