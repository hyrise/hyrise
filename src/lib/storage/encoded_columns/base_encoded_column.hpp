#pragma once

#include "column_encoding_type.hpp"

#include "storage/base_column.hpp"
#include "utils/assert.hpp"

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

  // Visitor pattern, see base_column.hpp
  void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) const override;

  virtual EncodingType encoding_type() const = 0;

  /**
   * @brief Returns the physical size of the vector
   */
  virtual size_t data_size() const = 0;

};

}  // namespace opossum
