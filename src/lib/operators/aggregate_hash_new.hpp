#pragma once

#include "abstract_aggregate_operator.hpp"

namespace opossum {

class AggregateHash : public AbstractAggregateOperator {  // TODO check how much of the abstract we use
 public:
  AggregateHash(const std::shared_ptr<AbstractOperator>& in, const std::vector<AggregateColumnDefinition>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;
};

}  // namespace opossum
