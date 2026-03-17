#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"

namespace hyrise {

class BaseRadixContainerWithStats;
class BloomFilter;

class Build : public AbstractReadOnlyOperator {
 public:
  Build(const std::shared_ptr<const AbstractOperator>& input_operator, const ColumnID column_id);

  const std::string& name() const override;

  ColumnID column_id() const;
  DataType data_type() const;
  const BaseRadixContainerWithStats& radix_container() const;
  const BloomFilter& bloom_filter() const;
  size_t radix_bits() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) override;

  ColumnID _column_id;
  DataType _data_type;
  BaseRadixContainerWithStats _radix_container;
  BloomFilter _bloom_filter;
  std::optional<size_t> _radix_bits;
};

}  // namespace hyrise
