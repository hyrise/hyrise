#pragma once

#include <fstream>
#include <string>
#include <vector>

#include "types.hpp"

namespace opossum {

class CsvWriter {
 public:
  /*
   * Creates a new CsvWriter with the given file as output file.
   * @param file The file to output the csv to.
   */
  explicit CsvWriter(const std::string& file);

  /*
   * Writes a cell with the content of an AllTypeVariant.
   */
  void write(const AllTypeVariant& value);
  /*
   * Ends a row of entries in the csv file.
   */
  void end_line();

  /*
   * Writes a full line of AllTypeVariants as a row.
   * Also calls end_line() to finish the row.
   */
  void write_line(const std::vector<AllTypeVariant>& values);

 protected:
  void escape(std::string& string);
  std::ofstream _stream;
  ColumnID _current_col_count = 0;
};
}  // namespace opossum
