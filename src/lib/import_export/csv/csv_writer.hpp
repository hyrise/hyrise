#pragma once

#include <fstream>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "csv_meta.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class CsvWriter {
 public:
  /*
   * Creates a new CsvWriter with the given file as output file.
   * @param file The file to output the csv to.
   */
  static void write(const Table& table, const std::string& filename, const ParseConfig& config = {});

  /*
   * Ends a row of entries in the csv file.
   */
  void end_line();

 protected:
  static void _generate_meta_info_file(const Table& table, const std::string& filename);
  static void _generate_content_file(const Table& table, const std::string& filename, const ParseConfig& config);
  static void _write(const AllTypeVariant& value, std::ofstream& ofstream, const ParseConfig& config);
  static pmr_string _escape(const pmr_string& string, const ParseConfig& config);
  static void _write_string_value(const pmr_string& value, std::ofstream& ofstream, const ParseConfig& config);
};

}  // namespace opossum
