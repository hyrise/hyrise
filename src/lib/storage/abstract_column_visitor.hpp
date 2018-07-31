#pragma once

#include <memory>

namespace opossum {

class BaseColumn;
class BaseDictionaryColumn;
class BaseEncodedColumn;
class BaseValueColumn;
class ReferenceColumn;

class ColumnVisitorContext {};

// In cases where an operator has to operate on different column types, we use the visitor pattern.
// By inheriting from AbstractColumnVisitor, an AbstractOperator(Impl) can implement handle methods for all column
// types.
class AbstractColumnVisitor {
 public:
  virtual ~AbstractColumnVisitor() = default;
  virtual void handle_column(const BaseValueColumn& column, std::shared_ptr<ColumnVisitorContext> context) = 0;
  virtual void handle_column(const BaseDictionaryColumn& column, std::shared_ptr<ColumnVisitorContext> context) = 0;
  virtual void handle_column(const ReferenceColumn& column, std::shared_ptr<ColumnVisitorContext> context) = 0;
  virtual void handle_column(const BaseEncodedColumn& column, std::shared_ptr<ColumnVisitorContext> context) = 0;
};

}  // namespace opossum
