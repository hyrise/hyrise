#pragma once

#include "abstract_table_constraint.hpp"

namespace opossum {

enum class KeyConstraintType : bool { PRIMARY_KEY, UNIQUE };

class TableKeyConstraint final : public AbstractTableConstraint {
 public:

  TableKeyConstraint(std::unordered_set<ColumnID> init_columns, KeyConstraintType init_key_type);

  KeyConstraintType key_type() const;

  bool operator==(const AbstractTableConstraint& rhs) const override;

 private:
  KeyConstraintType _key_type;
};

using TableKeyConstraints = std::vector<TableKeyConstraint>;

}  // namespace opossum
