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
  if (json.find("chunk_size") != json.end()) {
    auto chunk_size = json.at("chunk_size");
    Assert(static_cast<double>(chunk_size) == static_cast<size_t>(chunk_size),
           "CSV meta file, chunk_size must be an integer.");
    Assert(static_cast<int64_t>(chunk_size) > 0, "CSV meta file, chunk_size must greater than 0.");
    meta.chunk_size = chunk_size;
  }

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

  if (json.find("cxlumns") != json.end()) {
    Assert(json.at("cxlumns").is_array(), "CSV meta file,\"Cxlumns\" field has to be a json array.");
    for (const auto& cxlumn : json.at("cxlumns")) {
      CxlumnMeta cxlumn_meta;
      cxlumn_meta.name = cxlumn.at("name");
      cxlumn_meta.type = cxlumn.at("type");
      assign_if_exists(cxlumn_meta.nullable, cxlumn, "nullable");
      meta.cxlumns.push_back(cxlumn_meta);
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

  auto cxlumns = nlohmann::json::parse("[]");
  for (const auto& cxlumn_meta : meta.cxlumns) {
    cxlumns.emplace_back(
        nlohmann::json{{"name", cxlumn_meta.name}, {"type", cxlumn_meta.type}, {"nullable", cxlumn_meta.nullable}});
  }

  if (meta.chunk_size == Chunk::MAX_SIZE) {
    json = nlohmann::json{{"auto_compress", meta.auto_compress}, {"config", config}, {"cxlumns", cxlumns}};
  } else {
    json = nlohmann::json{{"chunk_size", meta.chunk_size},
                          {"auto_compress", meta.auto_compress},
                          {"config", config},
                          {"cxlumns", cxlumns}};
  }
}

bool operator==(const CxlumnMeta& left, const CxlumnMeta& right) {
  return std::tie(left.name, left.type, left.nullable) == std::tie(right.name, right.type, right.nullable);
}

bool operator==(const ParseConfig& left, const ParseConfig& right) {
  return std::tie(left.delimiter, left.separator, left.quote, left.escape, left.delimiter_escape,
                  left.reject_quoted_nonstrings,
                  left.rfc_mode) == std::tie(right.delimiter, right.separator, right.quote, right.escape,
                                             right.delimiter_escape, right.reject_quoted_nonstrings, right.rfc_mode);
}

bool operator==(const CsvMeta& left, const CsvMeta& right) {
  return std::tie(left.chunk_size, left.auto_compress, left.config, left.cxlumns) ==
         std::tie(right.chunk_size, right.auto_compress, right.config, right.cxlumns);
}

}  // namespace opossum
