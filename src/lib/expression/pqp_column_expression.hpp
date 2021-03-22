#pragma once

#include "abstract_expression.hpp"

#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Wraps a ColumnID and its associated data_type/nullability/column_name
 */
class PQPColumnExpression : public AbstractExpression {
 public:
  static std::shared_ptr<PQPColumnExpression> from_table(const Table& table, const std::string& column_name);
  static std::shared_ptr<PQPColumnExpression> from_table(const Table& table, const ColumnID column_id);

  PQPColumnExpression(const ColumnID init_column_id, const DataType data_type, const bool nullable,
                      const std::string& column_name);

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;
  bool requires_computation() const override;

  const ColumnID column_id;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const override;

 private:
  const DataType _data_type;
  const bool _nullable;
  const std::string _column_name;
};

}  // namespace opossum
