#pragma once

#include <string>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

struct TableColumnID {
  TableColumnID(const std::string init_table_name, const ColumnID init_column_id);
  std::string table_name;
  ColumnID column_id;
  std::string description() const;
  bool operator==(const TableColumnID& other) const;
  bool operator!=(const TableColumnID& other) const;
  std::string column_name() const;
  size_t hash() const;
};

const static TableColumnID INVALID_TABLE_COLUMN_ID = TableColumnID{"", INVALID_COLUMN_ID};

using TableColumnIDs = std::vector<TableColumnID>;

TableColumnID resolve_column_expression(const std::shared_ptr<const AbstractExpression>& column_expression);

std::ostream& operator<<(std::ostream& stream, const TableColumnID& table_column_id);

}  // namespace opossum

namespace std {

template <>
struct hash<opossum::TableColumnID> {
  size_t operator()(const opossum::TableColumnID& table_column_id) const;
};

}  // namespace std
