#pragma once

#include <string>
#include <vector>

#include "nlohmann/json.hpp"
#include "storage/chunk.hpp"

namespace opossum {

struct ColumnMeta {
  std::string name;
  std::string type;

  bool nullable = false;
};

// Strategies on how to deal with unquoted null Strings ("...,Null,...") in csv files:
// RejectNullStrings: An unquoted null string (case-insensitive) causes an exception - only empty
//  field is allowed as null value.
// NullStringAsValue: An unquoted null string is parsed as a string - eg. "Null", case is not changed.
// NullStringAsNull: An unquoted null string (case-insensitive) is parsed as a null value.
enum class NullHandling { RejectNullStrings, NullStringAsNull, NullStringAsValue };

struct ParseConfig {
  char delimiter = '\n';
  char separator = ',';
  char quote = '"';
  char escape = '"';
  char delimiter_escape = '\\';

  // If this is set to true, "4.3" will not be accepted as a value for a float column.
  bool reject_quoted_nonstrings = true;

  NullHandling null_handling = NullHandling::RejectNullStrings;

  // Indicator whether the Csv follows RFC 4180. (see https://tools.ietf.org/html/rfc4180)
  bool rfc_mode = true;

  static constexpr const char* NULL_STRING = "null";
};

/*
 * Meta information for a CSV table:
 *
 * config        characters and options that specify how the CSV should be parsed (delimiter, separator, etc.)
 * columns       column meta information (name, type, nullable) for each column
 */
struct CsvMeta {
  ParseConfig config;
  std::vector<ColumnMeta> columns;

  static constexpr const char* META_FILE_EXTENSION = ".json";
};

/*
 * This returns a CsvMeta object based on the content of the provided JSON file.
 * It takes all default values from the CsvMeta struct, and then overrides the ones that are provided in the JSON.
 */
CsvMeta process_csv_meta_file(const std::string& filename);

/*
 * Functions used internally when converting CsvMeta to nlohmann::json and the other way round:
 *
 * opossum::CsvMeta meta{};
 * nlohmann::json json = meta;
 *
 * nlohmann::json json = nlohmann::json::parse("{ ... }");
 * opossum::CsvMeta meta = json;
 */
void from_json(const nlohmann::json& json, CsvMeta& meta);
void to_json(nlohmann::json& json, const CsvMeta& meta);

/*
 * Equals-operator for convenience and use in tests.
 */
bool operator==(const CsvMeta& left, const CsvMeta& right);

}  // namespace opossum
