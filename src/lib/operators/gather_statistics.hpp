#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "expression/abstract_expression.hpp"
#include "all_type_variant.hpp"

namespace hyrise {

class GatherStatistics : public AbstractReadOnlyOperator {
 public:
  GatherStatistics(const std::shared_ptr<const AbstractOperator>& input_operator, const ColumnID column_id);

  const std::string& name() const override;

  ColumnID column_id() const;
  AllTypeVariant min_value() const;
  AllTypeVariant max_value() const;
  bool is_continuous() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) override;

 private:
  AllTypeVariant _min_value{NULL_VALUE};
  AllTypeVariant _max_value{NULL_VALUE};
  bool _is_continuous{false};
  ColumnID _column_id;
};

}  // namespace hyrise
