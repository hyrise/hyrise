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

  const char* meta_file_extension = ".meta";

  // Indicator whether the Csv follows RFC 4180. (see https://tools.ietf.org/html/rfc4180)
  bool rfc_mode = true;
};

}  // namespace opossum
