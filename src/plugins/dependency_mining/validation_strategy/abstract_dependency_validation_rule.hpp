#pragma once

#include <tbb/concurrent_unordered_map.h>
#include <mutex>

#include "dependency_mining/util.hpp"
#include "storage/abstract_table_constraint.hpp"

namespace opossum {

enum class DependencyValidationStatus { Valid, Invalid, Uncertain };

using ValidationResult = std::pair<DependencyValidationStatus,
                                   std::vector<std::pair<std::string, std::shared_ptr<AbstractTableConstraint>>>>;
const static ValidationResult INVALID_VALIDATION_RESULT{DependencyValidationStatus::Invalid, {}};
const static ValidationResult UNCERTAIN_VALIDATION_RESULT{DependencyValidationStatus::Uncertain, {}};

class AbstractDependencyValidationRule {
 public:
  AbstractDependencyValidationRule(
      const DependencyType type,
      tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes);
  virtual ~AbstractDependencyValidationRule() = default;

  // we assume not to get candidates multiple times here
  DependencyValidationStatus is_valid(const DependencyCandidate& candidate) const;
  const DependencyType dependency_type;

 protected:
  bool _is_known(const DependencyCandidate& candidate) const;
  virtual ValidationResult _on_validate(const DependencyCandidate& candidate) const = 0;
  void _add_constraint(const std::string& table_name, const AbstractTableConstraint& constraint) const;
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& _table_constraint_mutexes;
};

}  // namespace opossum
