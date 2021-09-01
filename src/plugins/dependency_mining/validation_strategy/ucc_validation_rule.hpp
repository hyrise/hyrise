#pragma once

#include "abstract_dependency_validation_rule.hpp"
#include "operators/aggregate_hash.hpp"

namespace opossum {

class UCCValidationRule : public AbstractDependencyValidationRule {
 public:
  UCCValidationRule();

 protected:
  std::shared_ptr<ValidationResult> _on_validate(const DependencyCandidate& candidate) const final override;

  template <typename AggregateKey>
  KeysPerChunk<AggregateKey> _partition_by_groupby_keys(const std::shared_ptr<const Table>& input_table,
                                                        const std::vector<ColumnID>& groupby_column_ids,
                                                        std::atomic_size_t& expected_result_size,
                                                        bool& use_immediate_key_shortcut) const;

  template <typename AggregateKey>
  DependencyValidationStatus _aggregate(const std::shared_ptr<const Table>& input_table,
                                        const std::vector<ColumnID>& groupby_column_ids) const;
};

}  // namespace opossum
