#pragma once

#include <optional>

#include "abstract_join_operator.hpp"
#include "operator_join_predicate.hpp"
#include "types.hpp"

namespace opossum {

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
  std::string description(DescriptionMode description_mode) const override;

  template <typename T>
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

template <typename T>
size_t JoinHash::calculate_radix_bits(const size_t build_side_size, const size_t probe_side_size, const JoinMode mode) {
  /*
    The number of radix bits is used to determine the number of build partitions. The idea is to size the partitions in
    a way that keeps the whole hash map cache resident. We aim for the largest unshared cache (for most Intel systems
    that's the L2 cache, for Apple's M1 the L1 cache). This calculation should include hardware knowledge, once
    available in Hyrise. As of now, we assume a cache size of 1024 KB, of which we use 75 %.

    We estimate the size the following way:
      - we assume each key appears once (that is an overestimation space-wise, but we
        aim rather for a hash map that is slightly smaller than the cache than slightly larger)
      - each entry in the hash map is a pair of the actual hash key and the SmallPosList storing uint32_t offsets (see
        hash_join_steps.hpp)
  */
  if (build_side_size > probe_side_size) {
    /*
      Hash joins perform best when the build side is small. For inner joins, we can simply select the smaller input
      table as the build side. For other joins, such as semi or outer joins, the build side is fixed. In this case,
      other join operators might be more efficient. We emit performance warning in this case. In the future, the
      optimizer could identify these cases of potentially inefficient hash joins and switch to other join algorithms.
    */
    PerformanceWarning("Build side larger than probe side in hash join");
  }

  // We assume a cache of 1024 KB for an Intel Xeon Platinum 8180. For local deployments or other CPUs, this size might
  // be different (e.g., an AMD EPYC 7F72 CPU has an L2 cache size of 512 KB and Apple's M1 has 128 KB).
  constexpr auto L2_CACHE_SIZE = 1'024'000;                   // bytes
  constexpr auto L2_CACHE_MAX_USABLE = L2_CACHE_SIZE * 0.75;  // use 75% of the L2 cache size

  // For information about the sizing of the bytell hash map, see the comments:
  // https://probablydance.com/2018/05/28/a-new-fast-hash-table-in-response-to-googles-new-fast-hash-table/
  // Bytell hash map has a maximum fill factor of 0.9375. Since it's hard to estimate the number of distinct values in
  // a radix partition (and thus the size of each hash table), we accomodate a little bit extra space for
  // slightly skewed data distributions and aim for a fill level of 80%.
  const auto complete_hash_map_size =
      // number of items in map
      static_cast<double>(build_side_size) *
      // key + value (and one byte overhead, see link above)
      static_cast<double>(sizeof(uint32_t)) / 0.8;

  const auto cluster_count = std::max(1.0, complete_hash_map_size / L2_CACHE_MAX_USABLE);

  return static_cast<size_t>(std::ceil(std::log2(cluster_count)));
}

}  // namespace opossum
