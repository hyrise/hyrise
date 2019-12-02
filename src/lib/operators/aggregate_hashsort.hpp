#pragma once

#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <boost/container/scoped_allocator.hpp>
#include <boost/functional/hash.hpp>

#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_aggregate_operator.hpp"
#include "aggregate/aggregate_hashsort_config.hpp"

namespace opossum {

class AggregateHashSort : public AbstractAggregateOperator {
 public:
  AggregateHashSort(const std::shared_ptr<AbstractOperator>& in,
                    const std::vector<AggregateColumnDefinition>& aggregates,
                    const std::vector<ColumnID>& groupby_column_ids, const std::optional<AggregateHashSortConfig>& config = {});

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

 private:
  AggregateHashSortConfig _config;
};

}  // namespace opossum
