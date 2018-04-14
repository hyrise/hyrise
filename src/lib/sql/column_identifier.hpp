#pragma once

#include <string>
#include <optional>

namespace opossum {

struct ColumnIdentifier final {
  ColumnIdentifier(const std::string& column_name, const std::optional<std::string>& table_name =
  std::nullopt);  // NOLINT - Implicit conversion is intended

  bool operator==(const ColumnIdentifier& rhs) const;

  std::string as_string() const;

  std::string column_name;
  std::optional<std::string> table_name = std::nullopt;
};

}  // namespace opossum
