#pragma once

#include "abstract_join_operator.hpp"

namespace opossum {

class AbstractCostEstimator;
class JoinNode;
enum class OperatorType;
/**
   * This operator joins two tables using one column of each table.
   *
   */
class JoinProxy : public AbstractJoinOperator {
 public:
  JoinProxy(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
            const JoinMode mode, const std::pair<ColumnID, ColumnID>& column_ids,
            const PredicateCondition predicate_condition);

  const std::string name() const override;

  struct PerformanceData : public OperatorPerformanceData {
    std::string to_string(DescriptionMode description_mode = DescriptionMode::SingleLine) const override;
  };

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  const std::shared_ptr<AbstractJoinOperator> _instantiate_join(const OperatorType operator_type);
  const std::vector<OperatorType> _valid_join_types() const;
  const std::shared_ptr<AbstractCostEstimator> _cost_model;
  std::optional<OperatorType> _operator_type;
};

}  // namespace opossum
