#pragma once

#include "abstract_dependency_validation_rule.hpp"

namespace opossum {

class INDValidationRule : public AbstractDependencyValidationRule {
 public:
  INDValidationRule();

 protected:
  struct ColumnStatistics {
    ColumnStatistics(const AllTypeVariant& init_min_value, const AllTypeVariant& init_max_value,
                     const float init_not_null_value_count)
        : min_value(init_min_value), max_value(init_max_value), not_null_value_count(init_not_null_value_count) {}
    AllTypeVariant min_value;
    AllTypeVariant max_value;
    float not_null_value_count;
  };
  std::shared_ptr<ValidationResult> _on_validate(const DependencyCandidate& candidate) const final;
};

}  // namespace opossum
