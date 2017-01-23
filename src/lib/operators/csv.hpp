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

constexpr auto file_extension = ".csv";
constexpr auto meta_file_extension = ".meta.csv";

}  // namespace csv
}  // namespace opossum
