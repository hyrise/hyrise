#include "csv_meta.hpp"

#include <fstream>
#include <string>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

CsvMeta process_csv_meta_file(const std::string& filename) {
  std::ifstream metafile{filename};
  Assert(metafile.good(), "Meta file does not exist: " + filename);
  nlohmann::json meta_json;
  metafile >> meta_json;
  return static_cast<CsvMeta>(meta_json);
}

void assign_if_exists(char& value, const nlohmann::json& json_object, const std::string& key) {
  if (json_object.find(key) != json_object.end()) {
    std::string character = json_object.at(key);
    Assert(character.size() == 1, "CSV meta file config: Character specifications can only be a single character.");
    value = character[0];
  }
}

void assign_if_exists(bool& value, const nlohmann::json& json_object, const std::string& key) {
  if (json_object.find(key) != json_object.end()) {
    value = json_object.at(key);
  }
}

void from_json(const nlohmann::json& json, CsvMeta& meta) {
  // Apply only parts of the ParseConfig that are provided, use default values otherwise
  ParseConfig config{};
  if (json.find("config") != json.end()) {
    Assert(json.at("config").is_object(), "CSV meta file,\"Config\" field has to be a json object.");
    nlohmann::json config_json = json.at("config");
    assign_if_exists(config.delimiter, config_json, "delimiter");
    assign_if_exists(config.separator, config_json, "separator");
    assign_if_exists(config.quote, config_json, "quote");
    assign_if_exists(config.escape, config_json, "escape");
    assign_if_exists(config.delimiter_escape, config_json, "delimiter_escape");
    assign_if_exists(config.reject_quoted_nonstrings, config_json, "reject_quoted_nonstrings");
    assign_if_exists(config.reject_null_strings, config_json, "reject_null_strings");
    assign_if_exists(config.unquoted_null_string_is_string, config_json, "unquoted_null_string_is_string");
    assign_if_exists(config.rfc_mode, config_json, "rfc_mode");
  }

  if (json.find("columns") != json.end()) {
    Assert(json.at("columns").is_array(), "CSV meta file,\"Columns\" field has to be a json array.");
    for (const auto& column : json.at("columns")) {
      ColumnMeta column_meta;
      column_meta.name = column.at("name");
      column_meta.type = column.at("type");
      assign_if_exists(column_meta.nullable, column, "nullable");
      meta.columns.push_back(column_meta);
    }
  }

  meta.config = config;
}

void to_json(nlohmann::json& json, const CsvMeta& meta) {
  nlohmann::json config = nlohmann::json{{"delimiter", std::string(1, meta.config.delimiter)},
                                         {"separator", std::string(1, meta.config.separator)},
                                         {"quote", std::string(1, meta.config.quote)},
                                         {"escape", std::string(1, meta.config.escape)},
                                         {"delimiter_escape", std::string(1, meta.config.delimiter_escape)},
                                         {"reject_quoted_nonstrings", meta.config.reject_quoted_nonstrings},
                                         {"reject_null_strings", meta.config.reject_null_strings},
                                         {"unquoted_null_string_is_string", meta.config.unquoted_null_string_is_string},
                                         {"rfc_mode", meta.config.rfc_mode}};

  auto columns = nlohmann::json::parse("[]");
  for (const auto& column_meta : meta.columns) {
    columns.emplace_back(
        nlohmann::json{{"name", column_meta.name}, {"type", column_meta.type}, {"nullable", column_meta.nullable}});
  }

  json = nlohmann::json{{"config", config}, {"columns", columns}};
}

bool operator==(const ColumnMeta& left, const ColumnMeta& right) {
  return std::tie(left.name, left.type, left.nullable) == std::tie(right.name, right.type, right.nullable);
}

bool operator==(const ParseConfig& left, const ParseConfig& right) {
  return std::tie(left.delimiter, left.separator, left.quote, left.escape, left.delimiter_escape,
                  left.reject_quoted_nonstrings, left.reject_null_strings, left.unquoted_null_string_is_string,
                  left.rfc_mode) == std::tie(right.delimiter, right.separator, right.quote, right.escape,
                                             right.delimiter_escape, right.reject_quoted_nonstrings,
                                             right.reject_null_strings, right.unquoted_null_string_is_string,
                                             right.rfc_mode);
}

bool operator==(const CsvMeta& left, const CsvMeta& right) {
  return std::tie(left.config, left.columns) == std::tie(right.config, right.columns);
}

}  // namespace opossum
