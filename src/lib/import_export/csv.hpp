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

}  // namespace opossum
