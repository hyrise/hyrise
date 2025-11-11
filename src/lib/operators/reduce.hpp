#pragma once

#include <chrono>
#include <map>
#include <memory>

#include "abstract_read_only_operator.hpp"
#include "operator_join_predicate.hpp"
#include "types.hpp"
#include "utils/bloom_filter.hpp"
#include "utils/min_max_predicate.hpp"

namespace hyrise {

enum class ReduceMode : uint8_t { Build, Probe, ProbeAndBuild };

enum class UseMinMax : bool { Yes = true, No = false };

enum class ReduceOperatorSteps : uint8_t { Iteration, OutputWriting, FilterMerging };

class Reduce : public AbstractReadOnlyOperator {
 public:
  explicit Reduce(const std::shared_ptr<const AbstractOperator>& left_input,
                  const std::shared_ptr<const AbstractOperator>& right_input, const OperatorJoinPredicate predicate,
                  const ReduceMode reduce_mode, const UseMinMax use_min_max, uint8_t filter_size_exponent = 0,
                  uint8_t block_size_exponent = 0, uint8_t k = 2);

  const std::string& name() const override;

  std::shared_ptr<BaseBloomFilter> get_bloom_filter() const;

  std::shared_ptr<BaseMinMaxPredicate> get_min_max_predicate() const;

  using OperatorSteps = ReduceOperatorSteps;

  struct PerformanceData : public OperatorPerformanceData<OperatorSteps> {};

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  template <UseMinMax use_min_max>
  std::shared_ptr<const Table> _execute_build();

  template <UseMinMax use_min_max>
  std::shared_ptr<const Table> _execute_probe();

  template <UseMinMax use_min_max>
  std::shared_ptr<const Table> _execute_probe_and_build();

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override;

  const OperatorJoinPredicate _predicate;
  std::shared_ptr<BaseBloomFilter> _bloom_filter;
  std::shared_ptr<BaseMinMaxPredicate> _min_max_predicate;
  const ReduceMode _reduce_mode;
  const UseMinMax _use_min_max;
  uint8_t _filter_size_exponent;
  uint8_t _block_size_exponent;
  uint8_t _k;
};

}  // namespace hyrise
