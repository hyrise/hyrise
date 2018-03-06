#pragma once

#include "storage/base_column.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

/**
 * @brief Base class of all encoded columns
 *
 * Since encoded columns are immutable, all member variables
 * of sub-classes should be declared const.
 */
class BaseEncodedColumn : public BaseColumn {
 public:
  // Encoded columns are immutable
  void append(const AllTypeVariant&) final;

  // calls the column-specific handler in an operator (visitor pattern)
  void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) const override;

  virtual EncodingType encoding_type() const = 0;
};

}  // namespace opossum
