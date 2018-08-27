#pragma once

#include <memory>

namespace opossum {

class BaseSegment;
class BaseDictionarySegment;
class BaseEncodedColumn;
class BaseValueSegment;
class ReferenceSegment;

class ColumnVisitorContext {};

// In cases where an operator has to operate on different column types, we use the visitor pattern.
// By inheriting from AbstractColumnVisitor, an AbstractOperator(Impl) can implement handle methods for all column
// types.
class AbstractColumnVisitor {
 public:
  virtual ~AbstractColumnVisitor() = default;
  virtual void handle_column(const BaseValueSegment& column, std::shared_ptr<ColumnVisitorContext> context) = 0;
  virtual void handle_column(const BaseDictionarySegment& column, std::shared_ptr<ColumnVisitorContext> context) = 0;
  virtual void handle_column(const ReferenceSegment& column, std::shared_ptr<ColumnVisitorContext> context) = 0;
  virtual void handle_column(const BaseEncodedColumn& column, std::shared_ptr<ColumnVisitorContext> context) = 0;
};

}  // namespace opossum
