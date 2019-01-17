#pragma once

#include <optional>
#include <string>

namespace opossum {

struct SQLIdentifier final {
  SQLIdentifier(const std::string& column_name, const std::optional<std::string>& table_name =
                                                    std::nullopt);  // NOLINT - Implicit conversion is intended

  bool operator==(const SQLIdentifier& rhs) const;

  std::string as_string() const;

  std::string column_name;
  std::optional<std::string> table_name = std::nullopt;
};

}  // namespace opossum
