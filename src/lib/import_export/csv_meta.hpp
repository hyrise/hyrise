#pragma once

#include <string>
#include <vector>

#include "json.hpp"

namespace opossum {

struct ColumnMeta {
  std::string name;
  std::string type;

  bool nullable = false;
};

struct ParseConfig {
  char delimiter = '\n';
  char separator = ',';
  char quote = '"';
  char escape = '"';
  char delimiter_escape = '\\';

  // If this is set to true, "4.3" will not be accepted as a value for a float column
  bool reject_quoted_nonstrings = true;

  // Indicator whether the Csv follows RFC 4180. (see https://tools.ietf.org/html/rfc4180)
  bool rfc_mode = true;

  static constexpr const char* NULL_STRING = "null";
};

/*
 * Meta information for a CSV table:
 *
 * chunk_size desired chunk size of the table
 * config     characters and options that specify how the CSV should be parsed (delimiter, separator, etc.)
 * columns    column meta information (name, type, nullable) for each column
 */
struct CsvMeta {
  size_t chunk_size;
  ParseConfig config;
  std::vector<ColumnMeta> columns;

  static constexpr const char* META_FILE_EXTENSION = ".json";
};

/*
 * This returns a CsvMeta object based on the content of the provided JSON file.
 * It takes all default values from the CsvMeta struct, and then overrides the ones which are provided in the JSON.
 */
CsvMeta process_csv_meta_file(const std::string& filename);

/*
 * Functions used internally when converting CsvMeta to nlohmann::json and the other way round:
 *
 * opossum::CsvMeta meta{};
 * nlohmann::json json = meta;
 *
 * nlohmann::json json = "{ ... }"_json;
 * opossum::CsvMeta meta = json;
 */
void from_json(const nlohmann::json& json, CsvMeta& meta);
void to_json(nlohmann::json& json, const CsvMeta& meta);

}  // namespace opossum
