#pragma once

namespace opossum {

/*
 * Predefined characters used for the csv file.
 */

constexpr char csv_delimiter = '\n';
constexpr char csv_separator = ',';
constexpr char csv_quote = '"';
constexpr char csv_escape = '"';
constexpr char csv_delimiter_escape = '\\';

constexpr auto csv_meta_file_extension = ".meta";

struct CsvConfig {
  char delimiter = csv_delimiter;
  char separator = csv_separator;
  char quote = csv_quote;
  char escape = csv_escape;
  char delimiter_escape = csv_delimiter_escape;

  const char *meta_file_extension = csv_meta_file_extension;
};

}  // namespace opossum
