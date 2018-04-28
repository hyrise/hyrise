#pragma once

#include "abstract_column_expression.hpp"

#include "types.hpp"
#include "storage/table.hpp"

namespace opossum {

class PQPColumnExpression : public AbstractColumnExpression {
 public:
  PQPColumnExpression(const ColumnID column_id, const DataType data_type, const bool nullable);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

  const ColumnID column_id;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;

 private:
  const DataType _data_type;
  const bool _nullable;
};


}  // namespace opossum
