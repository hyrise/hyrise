#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"

namespace opossum {

enum class ASTNodeType {
  Aggregate,
  Delete,
  DummyTable,
  Insert,
  Join,
  Limit,
  Mock,
  Predicate,
  Projection,
  Root,
  ShowColumns,
  ShowTables,
  Sort,
  StoredTable,
  Union,
  Update,
  Validate
};

enum class ASTChildSide { Left, Right };

struct NamedColumnReference {
  std::string column_name;
  std::optional<std::string> table_name = std::nullopt;

  std::string as_string() const;
};

class AbstractASTNode;

using ColumnOrigin = std::pair<std::shared_ptr<const AbstractASTNode>, ColumnID>;
using ColumnOrigins = std::vector<ColumnOrigin>;
using ColumnIDMapping = std::vector<ColumnID>;

}