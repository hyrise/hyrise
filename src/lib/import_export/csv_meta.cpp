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
  auto chunk_size = json.at("chunk_size");
  Assert(static_cast<double>(chunk_size) == static_cast<size_t>(chunk_size),
         "CSV meta file, chunk_size must be an integer.");
  Assert(static_cast<int64_t>(chunk_size) >= 0, "CSV meta file, chunk_size must not be negative.");
  meta.chunk_size = chunk_size;

  assign_if_exists(meta.auto_compress, json, "auto_compress");

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
                                         {"rfc_mode", meta.config.rfc_mode}};

  auto columns = nlohmann::json::parse("[]");
  for (const auto& column_meta : meta.columns) {
    columns.emplace_back(
        nlohmann::json{{"name", column_meta.name}, {"type", column_meta.type}, {"nullable", column_meta.nullable}});
  }

  json = nlohmann::json{{"chunk_size", meta.chunk_size}, {"auto_compress", meta.auto_compress}, {"config", config}, {"columns", columns}};
}

bool operator==(const ColumnMeta& lhs, const ColumnMeta& rhs) {
  return std::tie(lhs.name, lhs.type, lhs.nullable) == std::tie(rhs.name, rhs.type, rhs.nullable);
}

bool operator==(const ParseConfig& lhs, const ParseConfig& rhs) {
  return std::tie(lhs.delimiter, lhs.separator, lhs.quote, lhs.escape, lhs.delimiter_escape,
                  lhs.reject_quoted_nonstrings,
                  lhs.rfc_mode) == std::tie(rhs.delimiter, rhs.separator, rhs.quote, rhs.escape, rhs.delimiter_escape,
                                            rhs.reject_quoted_nonstrings, rhs.rfc_mode);
}

bool operator==(const CsvMeta& lhs, const CsvMeta& rhs) {
  return std::tie(lhs.chunk_size, lhs.auto_compress, lhs.config, lhs.columns) == std::tie(rhs.chunk_size, rhs.auto_compress, rhs.config, rhs.columns);
}

}  // namespace opossum
