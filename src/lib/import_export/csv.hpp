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

  const char *meta_file_extension = ".meta";
};

}  // namespace opossum
