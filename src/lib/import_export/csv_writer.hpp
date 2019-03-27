#pragma once

#include <fstream>
#include <string>
#include <vector>

#include "all_type_variant.hpp"
#include "csv_meta.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

class CsvWriter {
 public:
  /*
   * Creates a new CsvWriter with the given file as output file.
   * @param file The file to output the csv to.
   */
  explicit CsvWriter(const std::string& file, const ParseConfig& config = {});

  void write(const AllTypeVariant& value);

  /*
   * Ends a row of entries in the csv file.
   */
  void end_line();

 protected:
  pmr_string _escape(const pmr_string& string);

  void _write_value(const AllTypeVariant& value);
  void _write_string_value(const pmr_string& value);

  std::ofstream _stream;
  ColumnID _current_column_count{0};
  ParseConfig _config;
};

}  // namespace opossum
