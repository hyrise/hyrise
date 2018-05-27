#pragma once

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class ArrayExpression : public AbstractExpression {
 public:
  explicit ArrayExpression(const std::vector<std::shared_ptr<AbstractExpression>>& elements);

  DataType data_type() const override;
  bool is_nullable() const override;

  /**
   * @return  The common DataType of the elements of the Array, or std::nullopt if none such exists
   */
  std::optional<DataType> common_element_data_type() const;

  const std::vector<std::shared_ptr<AbstractExpression>>& elements() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
};

}  // namespace opossum
