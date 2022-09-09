#pragma once

#include <optional>

#include "abstract_join_operator.hpp"
#include "operator_join_predicate.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * This operator joins two tables using one column of each table.
 * The output is a new table with referenced columns for all columns of the two inputs and filtered pos_lists.
 *
 * As with most operators, we do not guarantee a stable operation with regards to positions -
 * i.e., your sorting order might be disturbed.
 *
 * Find more information in our Wiki: https://github.com/hyrise/hyrise/wiki/Hash-Join-Operator
 */
class JoinHash : public AbstractJoinOperator {
 public:
  static bool supports(const JoinConfiguration config);

  // The jobs that perform the actual materialization, radix partitioning, building, and probing are added to the
  // scheduler in case the number of elements to process is above JOB_SPAWN_THRESHOLD. If not, the job is executed
  // directly. This threshold needs to be re-evaluated over time to find the value which gives the best performance.
  static constexpr auto JOB_SPAWN_THRESHOLD = 500;

  JoinHash(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
           const JoinMode mode, const OperatorJoinPredicate& primary_predicate,
           const std::vector<OperatorJoinPredicate>& secondary_predicates = {},
           const std::optional<size_t>& radix_bits = std::nullopt);

  const std::string& name() const override;

  static size_t calculate_radix_bits(const size_t build_side_size, const size_t probe_side_size, const JoinMode mode);

  enum class OperatorSteps : uint8_t {
    BuildSideMaterializing,
    ProbeSideMaterializing,
    Clustering,
    Building,
    Probing,
    OutputWriting
  };

  struct PerformanceData : public OperatorPerformanceData<OperatorSteps> {
    void output_to_stream(std::ostream& stream, DescriptionMode description_mode) const override;

    size_t radix_bits{0};
    // Initially, the left input is the build side and the right side is the probe side.
    bool left_input_is_build_side{true};

    // Due to the used Bloom filters, the number of actually joined tuples can significantly differ from the sizes of
    // the input tables. To enable analyses of the Bloom filter efficiency, we store the number of values that were
    // eventually materialized; i.e., "input_row_count - filtered_values_by_Bloom_filter".
    size_t build_side_materialized_value_count{0};
    size_t probe_side_materialized_value_count{0};

    // In build(), the Bloom filter potentially reduces the distinct values in the hash table (i.e., the size of the
    // hash table) and the number of rows (in case of non-semi/anti* joins).
    // Note, depending on the order of materialization, build_side_materialized_value_count is not necessarily equal to
    // build_side_position_count (see order of materialization in hash_join.cpp).
    size_t hash_tables_distinct_value_count{0};
    std::optional<size_t> hash_tables_position_count;
  };

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_cleanup() override;

  std::unique_ptr<AbstractReadOnlyOperatorImpl> _impl;
  std::optional<size_t> _radix_bits;

  template <typename LeftType, typename RightType>
  class JoinHashImpl;
  template <typename LeftType, typename RightType>
  friend class JoinHashImpl;
};

}  // namespace hyrise
