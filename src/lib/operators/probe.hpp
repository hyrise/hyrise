#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "abstract_join_operator.hpp"
#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace hyrise {

class Probe : public AbstractJoinOperator {
 public:
  static bool supports(const JoinConfiguration& config);

  Probe(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
        const OperatorJoinPredicate& primary_predicate);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
};

}  // namespace hyrise
