#pragma once

#include <memory>

#include "storage/base_deprecated_dictionary_column.hpp"
#include "storage/base_value_column.hpp"

namespace opossum {

class BaseColumn;
class ReferenceColumn;
class BaseDictionaryColumn;
class BaseDeprecatedDictionaryColumn;
class BaseEncodedColumn;

// In cases where an operator has to operate on different column types, we use the visitor pattern.
// By inheriting from ColumnVisitable, an AbstractOperator(Impl) can implement handle methods for all column
// types. Unfortunately, we cannot easily overload handle() because ValueColumn<T> is templated.
class ColumnVisitableContext {};
class ColumnVisitable {
 public:
  virtual ~ColumnVisitable() = default;
  virtual void handle_column(const BaseValueColumn& column, std::shared_ptr<ColumnVisitableContext> context) = 0;
  virtual void handle_column(const BaseDeprecatedDictionaryColumn& column,
                             std::shared_ptr<ColumnVisitableContext> context) = 0;
  virtual void handle_column(const BaseDictionaryColumn& column, std::shared_ptr<ColumnVisitableContext> context) = 0;
  virtual void handle_column(const ReferenceColumn& column, std::shared_ptr<ColumnVisitableContext> context) = 0;

  /**
   * This method is going to be called for all encoded column classes
   * that do not overload visit() themselves.
   */
  virtual void handle_column(const BaseEncodedColumn& column, std::shared_ptr<ColumnVisitableContext> context) = 0;
};

}  // namespace opossum
