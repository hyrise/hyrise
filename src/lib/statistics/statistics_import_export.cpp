#include "statistics_import_export.hpp"

#include <fstream>

#include "column_statistics.hpp"
#include "constant_mappings.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

TableStatistics import_table_statistics(const std::string& path) {
  std::ifstream stream(path);
  Assert(stream.good(), std::string("Couldn't open file '") + path + "'");
  return import_table_statistics(stream);
}

void export_table_statistics(const TableStatistics& table_statistics, const std::string& path) {
  std::ofstream stream(path);
  Assert(stream.good(), std::string("Couldn't open file '") + path + "'");
  export_table_statistics(table_statistics, stream);
}

TableStatistics import_table_statistics(std::istream& stream) {
  nlohmann::json json;
  stream >> json;
  return import_table_statistics(json);
}

void export_table_statistics(const TableStatistics& table_statistics, std::ostream& stream) {
  const auto json = export_table_statistics(table_statistics);
  stream << json;
}

TableStatistics import_table_statistics(const nlohmann::json& json) {
  const auto table_type_iter = table_type_to_string.right.find(json["table_type"].get<std::string>());
  Assert(table_type_iter != table_type_to_string.right.end(), "No such TableType");

  const auto table_type = table_type_iter->second;
  const auto row_count = json["row_count"].get<float>();

  const auto column_statistics_jsons = json["column_statistics"];
  Assert(column_statistics_jsons.is_array(), "ColumnStatistics should be stored in an array");

  std::vector<std::shared_ptr<const BaseColumnStatistics>> column_statistics;
  column_statistics.reserve(column_statistics_jsons.size());

  for (const auto& column_statistics_json : column_statistics_jsons) {
    column_statistics.emplace_back(import_column_statistics(column_statistics_json));
  }

  return {table_type, row_count, column_statistics};
}

std::shared_ptr<BaseColumnStatistics> import_column_statistics(const nlohmann::json& json) {
  const auto distinct_count = json["distinct_count"].get<float>();
  const auto null_value_ratio = json["null_value_ratio"].get<float>();

  const auto data_type_iter = data_type_to_string.right.find(json["data_type"].get<std::string>());
  Assert(data_type_iter != data_type_to_string.right.end(), "No such DataType");

  std::shared_ptr<BaseColumnStatistics> result_column_statistics;

  resolve_data_type(data_type_iter->second, [&](const auto type) {
    using ColumnDataType = typename decltype(type)::type;

    ColumnDataType min, max;

    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      min = static_cast<pmr_string>(json["min"].get<std::string>());
      max = static_cast<pmr_string>(json["max"].get<std::string>());
    } else {
      min = json["min"].get<ColumnDataType>();
      max = json["max"].get<ColumnDataType>();
    }

    result_column_statistics =
        std::make_shared<ColumnStatistics<ColumnDataType>>(null_value_ratio, distinct_count, min, max);
  });

  Assert(result_column_statistics, "resolve_data_type() apparently failed.");
  return result_column_statistics;
}

nlohmann::json export_table_statistics(const TableStatistics& table_statistics) {
  nlohmann::json table_statistics_json;

  table_statistics_json["table_type"] = table_type_to_string.left.at(table_statistics.table_type());
  table_statistics_json["row_count"] = table_statistics.row_count();
  table_statistics_json["column_statistics"] = nlohmann::json::array();

  for (const auto& column_statistics : table_statistics.column_statistics()) {
    table_statistics_json["column_statistics"].push_back(export_column_statistics(*column_statistics));
  }
  return table_statistics_json;
}

nlohmann::json export_column_statistics(const BaseColumnStatistics& base_column_statistics) {
  nlohmann::json column_statistics_json;
  column_statistics_json["data_type"] = data_type_to_string.left.at(base_column_statistics.data_type());
  column_statistics_json["distinct_count"] = base_column_statistics.distinct_count();
  column_statistics_json["null_value_ratio"] = base_column_statistics.null_value_ratio();

  resolve_data_type(base_column_statistics.data_type(), [&](const auto type) {
    using ColumnDataType = typename decltype(type)::type;
    const auto& column_statistics = static_cast<const ColumnStatistics<ColumnDataType>&>(base_column_statistics);
    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      column_statistics_json["min"] = static_cast<std::string>(column_statistics.min());
      column_statistics_json["max"] = static_cast<std::string>(column_statistics.max());
    } else {
      column_statistics_json["min"] = column_statistics.min();
      column_statistics_json["max"] = column_statistics.max();
    }
  });

  return column_statistics_json;
}

}  // namespace opossum
