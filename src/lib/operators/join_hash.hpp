#pragma once

#include "abstract_join_operator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * This operator joins two tables using one column of each table.
 * The output is a new table with referenced columns for all columns of the two inputs and filtered pos_lists.
 * If you want to filter by multiple criteria, you can chain this operator.
 *
 * As with most operators, we do not guarantee a stable operation with regards to positions -
 * i.e., your sorting order might be disturbed.
 *
 * Find more information in our Wiki: https://github.com/hyrise/hyrise/wiki/Radix-Partitioned-and-Hash-Based-Join
 */
class JoinHash : public AbstractJoinOperator {
 public:
  JoinHash(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
           const JoinMode mode, const ColumnIDPair& column_ids, const PredicateCondition predicate_condition,
           const std::optional<size_t>& radix_bits = std::nullopt);

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_cleanup() override;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;
  const std::optional<size_t> _radix_bits;

  template <typename LeftType, typename RightType>
  class JoinHashImpl;
};

}  // namespace opossum
