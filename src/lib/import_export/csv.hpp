#pragma once

namespace opossum {
namespace csv {

/*
 * Predefined characters used for the csv file.
 */

constexpr char delimiter = '\n';
constexpr char separator = ',';
constexpr char quote = '"';
constexpr char escape = '"';
constexpr char delimiter_escape = '\\';

constexpr auto meta_file_extension = ".meta";

}  // namespace csv
}  // namespace opossum
