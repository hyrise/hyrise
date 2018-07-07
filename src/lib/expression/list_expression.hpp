#pragma once

#include "abstract_expression.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class ListExpression : public AbstractExpression {
 public:
  explicit ListExpression(const std::vector<std::shared_ptr<AbstractExpression>>& elements);

  DataType data_type() const override;
  bool is_nullable() const override;

  const std::vector<std::shared_ptr<AbstractExpression>>& elements() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
};

}  // namespace opossum
