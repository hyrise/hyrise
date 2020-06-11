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
  static bool supports(JoinMode join_mode, PredicateCondition predicate_condition, DataType left_data_type,
                       DataType right_data_type, bool secondary_predicates);

  JoinProxy(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
            const JoinMode mode, const OperatorJoinPredicate& primary_predicate,
            const std::vector<OperatorJoinPredicate>& secondary_predicates = {});

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  const std::shared_ptr<AbstractJoinOperator> _instantiate_join(const OperatorType operator_type);
  const std::vector<OperatorType> _valid_join_types() const;
  std::shared_ptr<AbstractCostEstimator> _cost_model;
  std::optional<OperatorType> _operator_type;
};

}  // namespace opossum
