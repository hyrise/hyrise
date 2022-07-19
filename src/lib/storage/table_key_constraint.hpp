#pragma once

#include "types.hpp"
#include "abstract_table_constraint.hpp"

namespace opossum {

enum class KeyConstraintType { PRIMARY_KEY, UNIQUE };

/**
 * Container class to define uniqueness constraints for tables.
 * As defined by SQL, two types of keys are supported: PRIMARY KEY and UNIQUE keys.
 */
class TableKeyConstraint final : public AbstractTableConstraint {
 public:
  TableKeyConstraint(std::unordered_set<ColumnID> init_columns, KeyConstraintType init_key_type, std::optional<CommitID> latest_validated_commit = std::nullopt);

  KeyConstraintType key_type() const;
  std::optional<CommitID> latest_validated_commit() const;

 protected:
  bool _on_equals(const AbstractTableConstraint& table_constraint) const override;

 private:
  KeyConstraintType _key_type;
  std::optional<CommitID> _latest_validated_commit{};
};

using TableKeyConstraints = std::vector<TableKeyConstraint>;

}  // namespace opossum
