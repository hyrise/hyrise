#pragma once

#include "abstract_table_constraint.hpp"

namespace opossum {

enum class KeyConstraintType : bool { PRIMARY_KEY, UNIQUE };

/**
 * Container class to define key constraints across a set of column ids. As defined by the SQL standard, the following
 * two types are supported: PRIMARY KEY and UNIQUE key constraints.
 */
class TableKeyConstraint final : public AbstractTableConstraint {
 public:
  TableKeyConstraint(std::unordered_set<ColumnID> init_columns, KeyConstraintType init_key_type);

  KeyConstraintType key_type() const;

 protected:
  bool _on_shallow_equals(const AbstractTableConstraint& table_constraint) const override;

 private:
  KeyConstraintType _key_type;
};

using TableKeyConstraints = std::vector<TableKeyConstraint>;

}  // namespace opossum
