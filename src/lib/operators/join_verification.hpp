#pragma once

#include "operators/abstract_join_operator.hpp"

namespace opossum {

/**
 * Reference implementation of all Join operations that Hyrise supports, providing a "ground truth" to test the
 * "real" join operators (JoinHash etc.).
 * Designed for readability/verifiability and not for performance.
 */
class JoinVerification : public AbstractJoinOperator {
 public:
  static bool supports(JoinMode join_mode, PredicateCondition predicate_condition, DataType left_data_type,
                       DataType right_data_type, bool secondary_predicates);

  using Tuple = std::vector<AllTypeVariant>;

  JoinVerification(const std::shared_ptr<const AbstractOperator>& left,
                   const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                   const OperatorJoinPredicate& primary_predicate,
                   const std::vector<OperatorJoinPredicate>& secondary_predicates = {});

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  bool _tuples_match(const Tuple& tuple_left, const Tuple& tuple_right) const;
  bool _evaluate_predicate(const OperatorJoinPredicate& predicate, const Tuple& tuple_left,
                           const Tuple& tuple_right) const;
};

}  // namespace opossum
