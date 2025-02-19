#pragma once

#include <atomic>
#include <memory>

#include "abstract_read_only_operator.hpp"
#include "types.hpp"

namespace hyrise {

class Reduce : public AbstractReadOnlyOperator {
  friend class OperatorsReduceTest;

 public:
  explicit Reduce(const std::shared_ptr<const AbstractOperator>& input_relation, const ColumnID column_id);

  const std::string& name() const override;

  const std::shared_ptr<std::vector<std::atomic_uint64_t>>& export_filter() const;

  void import_filter(const std::shared_ptr<std::vector<std::atomic_uint64_t>>& filter);

//  protected:
  std::shared_ptr<const Table> _on_execute() override;

  void _create_filter();

  std::shared_ptr<Table> _execute_filter();

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _set_bit(uint32_t hash_22bit);

  bool _get_bit(uint32_t hash_22bit) const;

  std::shared_ptr<std::vector<std::atomic_uint64_t>> _filter;
  ColumnID _column_id;

  const uint32_t _filter_size = 65536;
};

}  // namespace hyrise