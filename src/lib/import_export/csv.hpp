#pragma once

namespace opossum {

/*
 * Predefined characters used for the csv file
 */

struct CsvConfig {
  char delimiter = '\n';
  char separator = ',';
  char quote = '"';
  char escape = '"';
  char delimiter_escape = '\\';

  // If this is set to true, "4.3" will not be accepted as a value for a float column
  bool reject_quoted_nonstrings = true;

  const char* meta_file_extension = ".meta";

  // Indicator whether the Csv follows RFC 4180. (see https://tools.ietf.org/html/rfc4180)
  bool rfc_mode = true;

  static constexpr const char* NULL_STRING = "null";
};

}  // namespace opossum
