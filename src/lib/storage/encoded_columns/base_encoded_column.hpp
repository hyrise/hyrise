#pragma once

#include "column_encoding_type.hpp"

#include "storage/base_column.hpp"
#include "utils/assert.hpp"


namespace opossum {

/**
 * @brief Base class of all encoded columns
 */
class BaseEncodedColumn : public BaseColumn {
 public:
  // Encoded columns are immutable
  void append(const AllTypeVariant&) final;

  // Visitor pattern, see base_column.hpp
  void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) const;

  virtual EncodingType encoding_type() const = 0;
};

}  // namespace opossum
