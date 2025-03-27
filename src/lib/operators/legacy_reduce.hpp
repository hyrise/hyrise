#pragma once

#include <memory>
#include <boost/dynamic_bitset.hpp>

#include "abstract_read_only_operator.hpp"
#include "operator_join_predicate.hpp"
#include "types.hpp"

namespace hyrise {

class LegacyReduce : public AbstractReadOnlyOperator {
 public:

  static constexpr auto BLOOM_FILTER_SIZE = 1 << 20;
  static constexpr auto BLOOM_FILTER_MASK = BLOOM_FILTER_SIZE - 1;
  using BloomFilter = boost::dynamic_bitset<>;
//   static const auto ALL_TRUE_BLOOM_FILTER = ~BloomFilter(BLOOM_FILTER_SIZE);

  explicit LegacyReduce(const std::shared_ptr<const AbstractOperator>& left_input,
                  const std::shared_ptr<const AbstractOperator>& right_input, const OperatorJoinPredicate predicate,
                  const bool update_filter);

  const std::string& name() const override;

  const std::shared_ptr<BloomFilter>& export_filter() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  void _create_filter(const std::shared_ptr<const Table>& table, const ColumnID column_id);

  std::shared_ptr<Table> _create_reduced_table();

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  std::shared_ptr<BloomFilter> _filter;
  const OperatorJoinPredicate _predicate;
  const bool _update_filter = true;
};

}  // namespace hyrise