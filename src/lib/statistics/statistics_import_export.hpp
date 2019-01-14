#pragma once


#include "json.hpp" // NEEDEDINCLUDE

#include "table_statistics.hpp" // NEEDEDINCLUDE

namespace opossum {

TableStatistics import_table_statistics(const std::string& path);
void export_table_statistics(const TableStatistics& table_statistics, const std::string& path);

TableStatistics import_table_statistics(std::istream& stream);
void export_table_statistics(const TableStatistics& table_statistics, std::ostream& stream);

TableStatistics import_table_statistics(const nlohmann::json& json);
std::shared_ptr<BaseColumnStatistics> import_column_statistics(const nlohmann::json& json);

nlohmann::json export_table_statistics(const TableStatistics& table_statistics);
nlohmann::json export_column_statistics(const BaseColumnStatistics& base_column_statistics);
}  // namespace opossum
