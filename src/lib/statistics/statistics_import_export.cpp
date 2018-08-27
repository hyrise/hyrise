#include "statistics_import_export.hpp"

#include <fstream>

#include "cxlumn_statistics.hpp"
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

  const auto cxlumn_statistics_jsons = json["cxlumn_statistics"];
  Assert(cxlumn_statistics_jsons.is_array(), "CxlumnStatistics should be stored in an array");

  std::vector<std::shared_ptr<const BaseCxlumnStatistics>> cxlumn_statistics;
  cxlumn_statistics.reserve(cxlumn_statistics_jsons.size());

  for (const auto& cxlumn_statistics_json : cxlumn_statistics_jsons) {
    cxlumn_statistics.emplace_back(import_cxlumn_statistics(cxlumn_statistics_json));
  }

  return {table_type, row_count, cxlumn_statistics};
}

std::shared_ptr<BaseCxlumnStatistics> import_cxlumn_statistics(const nlohmann::json& json) {
  const auto distinct_count = json["distinct_count"].get<float>();
  const auto null_value_ratio = json["null_value_ratio"].get<float>();

  const auto data_type_iter = data_type_to_string.right.find(json["data_type"].get<std::string>());
  Assert(data_type_iter != data_type_to_string.right.end(), "No such DataType");

  std::shared_ptr<BaseCxlumnStatistics> result_cxlumn_statistics;

  resolve_data_type(data_type_iter->second, [&](const auto type) {
    using CxlumnDataType = typename decltype(type)::type;
    const auto min = json["min"].get<CxlumnDataType>();
    const auto max = json["max"].get<CxlumnDataType>();

    result_cxlumn_statistics =
        std::make_shared<CxlumnStatistics<CxlumnDataType>>(null_value_ratio, distinct_count, min, max);
  });

  Assert(result_cxlumn_statistics, "resolve_data_type() apparently failed.");
  return result_cxlumn_statistics;
}

nlohmann::json export_table_statistics(const TableStatistics& table_statistics) {
  nlohmann::json table_statistics_json;

  table_statistics_json["table_type"] = table_type_to_string.left.at(table_statistics.table_type());
  table_statistics_json["row_count"] = table_statistics.row_count();
  table_statistics_json["cxlumn_statistics"] = nlohmann::json::array();

  for (const auto& cxlumn_statistics : table_statistics.cxlumn_statistics()) {
    table_statistics_json["cxlumn_statistics"].push_back(export_cxlumn_statistics(*cxlumn_statistics));
  }
  return table_statistics_json;
}

nlohmann::json export_cxlumn_statistics(const BaseCxlumnStatistics& base_cxlumn_statistics) {
  nlohmann::json cxlumn_statistics_json;
  cxlumn_statistics_json["data_type"] = data_type_to_string.left.at(base_cxlumn_statistics.data_type());
  cxlumn_statistics_json["distinct_count"] = base_cxlumn_statistics.distinct_count();
  cxlumn_statistics_json["null_value_ratio"] = base_cxlumn_statistics.null_value_ratio();

  resolve_data_type(base_cxlumn_statistics.data_type(), [&](const auto type) {
    using CxlumnDataType = typename decltype(type)::type;
    const auto& cxlumn_statistics = static_cast<const CxlumnStatistics<CxlumnDataType>&>(base_cxlumn_statistics);
    cxlumn_statistics_json["min"] = cxlumn_statistics.min();
    cxlumn_statistics_json["max"] = cxlumn_statistics.max();
  });

  return cxlumn_statistics_json;
}

}  // namespace opossum
