#pragma once

#include <string>

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

  const char* meta_file_extension = ".meta";

  // Indicator whether the Csv follows RFC 4180. (see https://tools.ietf.org/html/rfc4180)
  bool rfc_mode = true;
  
  static const std::string NULL_STRING;
  static const std::string COLUMN_TYPE;
  static const std::string NULLABLE_COLUMN_TYPE;
};

}  // namespace opossum
