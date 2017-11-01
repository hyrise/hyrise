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
 * Predefined characters used for the csv file
 */

struct CsvMeta {
  size_t chunk_size;
  ParseConfig config;
  std::vector<ColumnMeta> columns;

  static constexpr const char* META_FILE_EXTENSION = ".json";
};

void from_json(const nlohmann::json& json, CsvMeta& meta);
void to_json(nlohmann::json& json, const CsvMeta& meta);

CsvMeta process_csv_meta_file(const std::string& filename);

} // namespace opossum
