#pragma once

#include "abstract_expression.hpp"

#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Wraps a CxlumnID and its associated data_type/nullability/cxlumn_name
 */
class PQPCxlumnExpression : public AbstractExpression {
 public:
  static std::shared_ptr<PQPCxlumnExpression> from_table(const Table& table, const std::string& cxlumn_name);

  PQPCxlumnExpression(const CxlumnID cxlumn_id, const DataType data_type, const bool nullable,
                      const std::string& cxlumn_name);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_cxlumn_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;
  bool requires_computation() const override;

  const CxlumnID cxlumn_id;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;

 private:
  const DataType _data_type;
  const bool _nullable;
  const std::string _cxlumn_name;
};

}  // namespace opossum
