#pragma once

namespace opossum {

/*
 * Predefined characters used for the csv file.
 */

constexpr char csvDelimiter = '\n';
constexpr char csvSeparator = ',';
constexpr char csvQuote = '"';
constexpr char csvEscape = '"';
constexpr char csvDelimiter_escape = '\\';

constexpr auto csvMeta_file_extension = ".meta";

}  // namespace opossum
