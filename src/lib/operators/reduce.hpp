#pragma once

#include <atomic>
#include <memory>

#include "abstract_read_only_operator.hpp"
#include "operator_join_predicate.hpp"
#include "types.hpp"

namespace hyrise {

template <template <typename> class Hasher, uint8_t FilterSize>
class Reduce : public AbstractReadOnlyOperator {
  friend class OperatorsReduceTest;

 public:
  explicit Reduce(const std::shared_ptr<const AbstractOperator>& left_input,
                  const std::shared_ptr<const AbstractOperator>& right_input, const OperatorJoinPredicate predicate,
                  const bool update_filter);

  const std::string& name() const override;

  const std::shared_ptr<std::vector<std::atomic_uint64_t>>& export_filter() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  void _create_filter(const std::shared_ptr<const Table>& table, const ColumnID column_id);

  std::shared_ptr<Table> _create_reduced_table();

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _set_bit(uint32_t hash_22bit);

  bool _get_bit(uint32_t hash_22bit) const;

  std::shared_ptr<std::vector<std::atomic_uint64_t>> _filter;
  const OperatorJoinPredicate _predicate;
  const bool _update_filter = true;

  // const uint32_t FILTER_SIZE = 262144;
  std::string _filter_size_string;
  std::string _hash_count;
};

}  // namespace hyrise