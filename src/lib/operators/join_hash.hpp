#pragma once

#include <optional>

#include "abstract_join_operator.hpp"
#include "operator_join_predicate.hpp"
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
  static bool supports(const JoinConfiguration config);

  JoinHash(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
           const JoinMode mode, const OperatorJoinPredicate& primary_predicate,
           const std::vector<OperatorJoinPredicate>& secondary_predicates = {},
           const std::optional<size_t>& radix_bits = std::nullopt);

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;

  template <typename T>
  static size_t calculate_radix_bits(const size_t build_relation_size, const size_t probe_relation_size);

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_cleanup() override;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;
  std::optional<size_t> _radix_bits;

  template <typename LeftType, typename RightType>
  class JoinHashImpl;
  template <typename LeftType, typename RightType>
  friend class JoinHashImpl;
};

}  // namespace opossum
